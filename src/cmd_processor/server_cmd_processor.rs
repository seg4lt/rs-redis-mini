use anyhow::bail;
use async_recursion::async_recursion;
use std::{
    collections::BTreeMap,
    time::{Duration, Instant},
};
use tokio::{
    io::AsyncWriteExt,
    net::tcp::WriteHalf,
    sync::{mpsc, oneshot},
};
use tracing::debug;

use crate::database::db_event::DbValueType;
use crate::{
    app_config::AppConfig,
    cmd_parser::server_command::ServerCommand,
    database::{db_event::StreamDbValueType, Database},
    replication::ReplicationEvent,
    resp_type::RESPType,
    LINE_ENDING,
};
use ServerCommand::*;

impl ServerCommand {
    #[async_recursion]
    pub async fn process_client_cmd(
        &self,
        tx_stack: &mut Vec<Vec<ServerCommand>>,
    ) -> anyhow::Result<Option<RESPType>> {
        let resp = match self {
            Ping => RESPType::SimpleString("PONG".to_string()),
            Echo(value) => RESPType::BulkString(value.clone()),
            Set { key, value, flags } => {
                Database::set(key, value, flags).await?;
                let resp_type = RESPType::SimpleString("OK".to_string());
                ReplicationEvent::Set {
                    key: key.to_string(),
                    value: value.clone(),
                    flags: flags.clone(),
                }
                .emit()
                .await?;
                resp_type
            }
            Get { key } => match Database::get(&key).await? {
                None => RESPType::NullBulkString,
                Some(value) => match value {
                    DbValueType::Integer(value) => RESPType::BulkString(value.to_string()),
                    DbValueType::String(value) => RESPType::BulkString(value),
                    DbValueType::Stream(_) => unimplemented!("streams not supported via GET"),
                },
            },
            Incr { key } => match Database::incr(&key).await {
                Ok(value) => match value {
                    DbValueType::Integer(i) => RESPType::Integer(i),
                    DbValueType::String(s) => RESPType::BulkString(s),
                    DbValueType::Stream(_) => unimplemented!("stream incr not supported"),
                },
                Err(e) => RESPType::Error(e.to_string()),
            },
            Info { .. } => {
                let is_master = AppConfig::is_master();
                let role = match is_master {
                    true => "master",
                    false => "slave",
                };
                let mut info_vec = vec!["# Replication".to_string(), format!("role:{}", role)];
                if is_master {
                    info_vec.push(format!("master_replid:{}", AppConfig::get_master_replid()));
                    info_vec.push(format!(
                        "master_repl_offset:{}",
                        AppConfig::get_master_repl_offset()
                    ));
                }
                RESPType::BulkString(info_vec.join(LINE_ENDING))
            }
            ReplConf { .. } => RESPType::SimpleString("OK".to_string()),
            PSync { .. } => {
                let replid = AppConfig::get_master_replid();
                let offset = AppConfig::get_master_repl_offset();
                let content = format!("+FULLRESYNC {replid} {offset}");
                RESPType::SimpleString(content)
            }
            Wait { .. } => self.process_wait_cmd().await?,
            Config { cmd, key } => {
                let cmd = cmd.to_lowercase();
                if cmd != "get" {
                    bail!("Only GET command is supported for CONFIG");
                }
                match key.as_str() {
                    "dir" => RESPType::Array(vec![
                        RESPType::BulkString("dir".to_string()),
                        RESPType::BulkString(AppConfig::get_rds_dir().to_string()),
                    ]),
                    "dbfilename" => RESPType::Array(vec![
                        RESPType::BulkString("dbfilename".to_string()),
                        RESPType::BulkString(AppConfig::get_rds_file_name().to_string()),
                    ]),
                    _ => bail!("CONFIG key not supported yet"),
                }
            }
            Keys(flag) => RESPType::Array(
                Database::keys(flag)
                    .await?
                    .iter()
                    .map(|key| RESPType::BulkString(key.to_owned()))
                    .collect(),
            ),
            Type(key) => RESPType::SimpleString(Database::get_type(key).await?),
            XAdd {
                stream_key,
                stream_id,
                key,
                value,
            } => match Database::xadd(stream_key, stream_id, key, value).await {
                Ok(value) => RESPType::BulkString(value),
                Err(err) => RESPType::Error(err),
            },
            XRange { .. } => self.process_xrange_cmd().await?,
            XRead { .. } => self.process_xread_cmd().await?,
            Multi => self.process_multi_cmd(tx_stack).await?,
            Exec => {
                if tx_stack.is_empty() {
                    let resp = RESPType::Error("ERR EXEC without MULTI".to_string());
                    return Ok(Some(resp));
                }
                let tx = tx_stack.pop().unwrap();
                let mut collect: Vec<RESPType> = vec![];
                for s_cmd in tx {
                    if let Some(resp) = s_cmd.process_client_cmd(tx_stack).await? {
                        collect.push(resp);
                    }
                }
                RESPType::Array(collect)
            }
            Discard => {
                if tx_stack.is_empty() {
                    RESPType::Error("ERR DISCARD without MULTI".to_string())
                } else {
                    let _ = tx_stack.pop();
                    RESPType::SimpleString("OK".to_string())
                }
            }
            CustomNewLine | ExitConn => {
                return Ok(None);
            }
        };
        Ok(Some(resp))
    }

    async fn process_multi_cmd(
        &self,
        tx_stack: &mut Vec<Vec<ServerCommand>>,
    ) -> anyhow::Result<RESPType> {
        tx_stack.push(vec![]);
        let resp = RESPType::SimpleString("OK".to_string());
        Ok(resp)
    }

    async fn process_xread_cmd(&self) -> anyhow::Result<RESPType> {
        let XRead(filters, block_ms) = self else {
            bail!("Not a xread cmd");
        };

        let mut updated_filters: Vec<(String, String)> = vec![];
        let mut iterable = filters.iter();
        let mut item = iterable.next();
        loop {
            match item {
                None => break,
                Some((stream_key, stream_id)) => {
                    let stream_id = match stream_id.as_str() == "$" {
                        true => Database::get_last_stream_id(stream_key).await?,
                        false => stream_id.clone(),
                    };
                    updated_filters.push((stream_key.clone(), stream_id.clone()));
                    item = iterable.next();
                }
            };
        }

        debug!(?updated_filters, "Updated filters");

        let resp = match block_ms {
            None => self.internal_process_xread_cmd(&updated_filters).await?,
            Some(ms) => match ms {
                0 => {
                    let mut resp = RESPType::NullBulkString;
                    while let RESPType::NullBulkString = resp {
                        tokio::time::sleep(Duration::from_millis(1000)).await;
                        resp = self.internal_process_xread_cmd(&updated_filters).await?
                    }
                    resp
                }
                ms => {
                    debug!(?ms, "Blocking for ms");
                    tokio::time::sleep(Duration::from_millis(*ms)).await;
                    debug!(?ms, "Blocking for ms finished");
                    self.internal_process_xread_cmd(&updated_filters).await?
                }
            },
        };
        debug!("Final response: {:?}", resp);
        let str = String::from_utf8(resp.as_bytes())?;
        debug!(?str, "Final String");
        Ok(resp)
    }

    async fn internal_process_xread_cmd(
        &self,
        filters: &Vec<(String, String)>,
    ) -> anyhow::Result<RESPType> {
        let db_value = Database::xread(filters).await?;
        let mut outer_arr_value = vec![];
        let mut got_value = false;
        db_value
            .into_iter()
            .for_each(|(stream_key, stream_values)| {
                let mut inner_arr_value = vec![];
                inner_arr_value.push(RESPType::BulkString(stream_key.clone()));

                stream_values.into_iter().for_each(|item| {
                    got_value = true;
                    let inner_resp = RESPType::Array(vec![
                        RESPType::BulkString(format!(
                            "{}-{}",
                            item.stream_id_ms_part, item.stream_id_seq_part
                        )),
                        RESPType::Array(vec![
                            RESPType::BulkString(item.key),
                            RESPType::BulkString(item.value),
                        ]),
                    ]);
                    inner_arr_value.push(RESPType::Array(vec![inner_resp]));
                });

                let outer_resp = RESPType::Array(inner_arr_value);
                outer_arr_value.push(outer_resp);
            });

        let resp = if got_value {
            RESPType::Array(outer_arr_value)
        } else {
            RESPType::NullBulkString
        };
        Ok(resp)
    }

    async fn process_xrange_cmd(&self) -> anyhow::Result<RESPType> {
        let XRange {
            stream_key,
            start,
            end,
        } = self
        else {
            bail!("Not a xrange cmd");
        };
        let db_value = Database::xrange(stream_key, start, end).await?;
        let mut map: BTreeMap<String, StreamDbValueType> = BTreeMap::new();
        db_value.into_iter().for_each(|item| {
            map.insert(
                format!("{}-{}", item.stream_id_ms_part, item.stream_id_seq_part),
                item,
            );
        });
        let mut outer_vec = vec![];
        map.into_iter().for_each(|(key, value)| {
            let resp = RESPType::Array(vec![
                RESPType::BulkString(key),
                RESPType::Array(vec![
                    RESPType::BulkString(value.key),
                    RESPType::BulkString(value.value),
                ]),
            ]);
            outer_vec.push(resp);
        });
        let final_resp = RESPType::Array(outer_vec);
        debug!("Final response: {:?}", final_resp);
        Ok(final_resp)
    }

    async fn process_wait_cmd(&self) -> anyhow::Result<RESPType> {
        let Wait {
            ack_wanted,
            timeout_ms,
        } = self
        else {
            bail!("Not a wait cmd");
        };
        let (replication_event_resp_emitter, replication_event_resp_listener) =
            oneshot::channel::<usize>();
        ReplicationEvent::GetNumOfReplicas {
            resp: replication_event_resp_emitter,
        }
        .emit()
        .await?;
        let num_replicas = replication_event_resp_listener.await?;

        if num_replicas == 0 {
            let resp_type = RESPType::Integer(num_replicas as i64);
            return Ok(resp_type);
        }
        let was_last_command_set = Database::was_last_command_set().await?;
        if was_last_command_set == false {
            let resp_type = RESPType::Integer(num_replicas as i64);
            return Ok(resp_type);
        }

        let (replication_event_resp_emitter, mut replication_event_resp_listener) =
            mpsc::channel::<usize>(10);
        debug!("(inside of wait): Emitting ReplicationEvent::GetAck");

        ReplicationEvent::GetAck {
            ack_wanted: *ack_wanted,
            resp: replication_event_resp_emitter,
        }
        .emit()
        .await?;

        let start = Instant::now();
        let mut acks_received = 0;

        while start.elapsed() < Duration::from_millis(*timeout_ms as u64)
            && acks_received < num_replicas
        {
            let value = &replication_event_resp_listener.try_recv();
            match value {
                Ok(_ack) => {
                    debug!("!@#: GOT ACK");
                    acks_received += 1;
                }
                Err(_) => {
                    debug!("!@#: NO ACK");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            }
        }
        let elapshed = start.elapsed();
        debug!(?elapshed, ?acks_received, ?num_replicas, "Acks received");
        let resp_type = RESPType::Integer(acks_received as i64);
        debug!("Final response: {:?}", resp_type);
        Ok(resp_type)
    }
}

pub async fn send_rds_file(writer: &mut WriteHalf<'_>) -> anyhow::Result<()> {
    use base64::prelude::*;
    let rds_content = b"UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==";
    let decoded = BASE64_STANDARD.decode(rds_content)?;
    let resp_type = RESPType::RDB(decoded);
    debug!(
        "Sending RDB file to client {:?}",
        resp_type.as_bytes().len()
    );
    writer.write_all(&resp_type.as_bytes()).await?;
    writer.flush().await?;
    Ok(())
}
