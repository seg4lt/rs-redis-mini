use std::{
    collections::BTreeMap,
    time::{Duration, Instant},
};

use anyhow::{bail, Context};
use tokio::{
    io::AsyncWriteExt,
    net::tcp::WriteHalf,
    sync::{mpsc, oneshot},
};
use tracing::debug;

use crate::{
    app_config::AppConfig,
    cmd_parser::server_command::ServerCommand,
    database::{
        db_event::{DatabaseEvent, StreamDbValueType},
        Database,
    },
    fdbg,
    replication::ReplicationEvent,
    resp_type::RESPType,
    LINE_ENDING,
};
use ServerCommand::*;

impl ServerCommand {
    pub async fn process_client_cmd(&self, writer: &mut WriteHalf<'_>) -> anyhow::Result<()> {
        match self {
            Ping => {
                let resp_type = RESPType::SimpleString("PONG".to_string());
                writer.write_all(&resp_type.as_bytes()).await?;
            }
            Echo(value) => {
                let resp_type = RESPType::BulkString(value.clone());
                writer.write_all(&resp_type.as_bytes()).await?;
            }
            Set { key, value, flags } => {
                Database::set_kv(key, value, flags).await?;
                let resp_type = RESPType::SimpleString("OK".to_string());
                writer.write_all(&resp_type.as_bytes()).await?;
                ReplicationEvent::Set {
                    key: key.to_string(),
                    value: value.to_string(),
                    flags: flags.clone(),
                }
                .emit()
                .await?;
            }
            Get { key } => {
                let (replication_event_emitter, db_event_resp_listener) =
                    oneshot::channel::<Option<String>>();
                let kv_cmd = DatabaseEvent::Get {
                    resp: replication_event_emitter,
                    key: key.to_owned(),
                };
                Database::emit(kv_cmd).await?;
                match db_event_resp_listener.await? {
                    None => {
                        let resp_type = RESPType::NullBulkString;
                        writer.write_all(&resp_type.as_bytes()).await?;
                    }
                    Some(value) => {
                        let resp_type = RESPType::BulkString(value);
                        writer.write_all(&resp_type.as_bytes()).await?;
                    }
                }
            }
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
                let info_string = RESPType::BulkString(info_vec.join(LINE_ENDING));
                writer.write_all(&info_string.as_bytes()).await?;
            }
            ReplConf { .. } => {
                let resp_type = RESPType::SimpleString("OK".to_string());
                writer.write_all(&resp_type.as_bytes()).await?;
            }
            Psync { .. } => {
                let replid = AppConfig::get_master_replid();
                let offset = AppConfig::get_master_repl_offset();
                let content = format!("+FULLRESYNC {replid} {offset}");
                let resp_type = RESPType::SimpleString(content);
                writer.write_all(&resp_type.as_bytes()).await?;
                writer.flush().await?;
                send_rds_file(writer).await?;
            }
            Wait { .. } => self.process_wait_cmd(writer).await?,
            Config { cmd, key } => {
                let cmd = cmd.to_lowercase();
                if cmd != "get" {
                    bail!("Only GET command is supported for CONFIG");
                }
                match key.as_str() {
                    "dir" => {
                        let resp_type = RESPType::Array(vec![
                            RESPType::BulkString("dir".to_string()),
                            RESPType::BulkString(AppConfig::get_rds_dir().to_string()),
                        ]);
                        writer.write_all(&resp_type.as_bytes()).await?;
                    }
                    "dbfilename" => {
                        let resp_type = RESPType::Array(vec![
                            RESPType::BulkString("dbfilename".to_string()),
                            RESPType::BulkString(AppConfig::get_rds_file_name().to_string()),
                        ]);
                        writer.write_all(&resp_type.as_bytes()).await?;
                    }
                    _ => bail!("CONFIG key not supported yet"),
                }
            }
            Keys(flag) => {
                let (tx, rx) = oneshot::channel::<Vec<String>>();
                Database::emit(DatabaseEvent::Keys {
                    resp: tx,
                    flag: flag.to_owned(),
                })
                .await?;
                let value = rx
                    .await?
                    .iter()
                    .map(|key| RESPType::BulkString(key.to_owned()))
                    .collect();
                let resp = RESPType::Array(value);
                writer.write_all(&resp.as_bytes()).await?;
                writer.flush().await?;
            }
            Type(key) => {
                let (tx, rx) = oneshot::channel::<String>();
                Database::emit(DatabaseEvent::Type {
                    resp: tx,
                    key: key.to_owned(),
                })
                .await?;
                let value = rx.await?;
                let resp = RESPType::SimpleString(value);
                writer.write_all(&resp.as_bytes()).await?;
                writer.flush().await?;
            }
            XAdd {
                stream_key,
                stream_id,
                key,
                value,
            } => {
                let (tx, rx) = oneshot::channel::<Result<String, String>>();
                Database::emit(DatabaseEvent::XAdd {
                    resp: tx,
                    stream_key: stream_key.clone(),
                    stream_id: stream_id.clone(),
                    key: key.clone(),
                    value: value.clone(),
                })
                .await?;
                let resp = match rx.await? {
                    Ok(value) => RESPType::BulkString(value),
                    Err(err) => RESPType::Error(err),
                };
                writer.write_all(&resp.as_bytes()).await?;
                writer.flush().await?;
            }
            XRange { .. } => self.process_xrange_cmd(writer).await?,
            XRead { .. } => self.process_xread_cmd(writer).await?,
            CustomNewLine | ExitConn => {}
        };
        Ok(())
    }

    async fn process_xread_cmd(&self, writer: &mut WriteHalf<'_>) -> anyhow::Result<()> {
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
                    debug!(?stream_key, ?stream_id, "Stream Key and Stream ID");
                    let stream_id = if stream_id.as_str() == "$" {
                        let (tx, rx) = oneshot::channel::<String>();
                        Database::emit(DatabaseEvent::_GetLastStreamId {
                            resp: tx,
                            stream_key: stream_key.clone(),
                        })
                        .await?;
                        let db_value = rx.await?;
                        debug!(?db_value, "Last Stream ID");
                        db_value
                    } else {
                        stream_id.clone()
                    };
                    updated_filters.push((stream_key.clone(), stream_id.clone()));
                    item = iterable.next();
                }
            };
        }

        debug!(?updated_filters, "Updated filters");

        let resp = match block_ms {
            None => self
                .internal_process_xread_cmd(&updated_filters)
                .await
                .unwrap(),
            Some(ms) => match ms {
                0 => {
                    let mut resp = RESPType::NullBulkString;
                    while let RESPType::NullBulkString = resp {
                        tokio::time::sleep(Duration::from_millis(1000)).await;
                        resp = self
                            .internal_process_xread_cmd(&updated_filters)
                            .await
                            .unwrap()
                    }
                    resp
                }
                ms => {
                    debug!(?ms, "Blocking for ms");
                    tokio::time::sleep(Duration::from_millis(*ms)).await;
                    debug!(?ms, "Blocking for ms finished");
                    self.internal_process_xread_cmd(&updated_filters)
                        .await
                        .unwrap()
                }
            },
        };
        debug!("Final response: {:?}", resp);
        let str = String::from_utf8(resp.as_bytes()).unwrap();
        debug!(?str, "Final String");
        writer.write_all(&resp.as_bytes()).await?;
        writer.flush().await?;
        Ok(())
    }

    async fn internal_process_xread_cmd(
        &self,
        filters: &Vec<(String, String)>,
    ) -> anyhow::Result<RESPType> {
        let (tx, rx) = oneshot::channel::<Vec<(String, Vec<StreamDbValueType>)>>();
        Database::emit(DatabaseEvent::XRead {
            resp: tx,
            filters: filters.clone(),
        })
        .await
        .context(fdbg!("Somethign wrong when sending read event"))?;

        let db_value = rx.await.context(fdbg!("Unable to receive"))?;

        debug!(?db_value, "DB Value");

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

    async fn process_xrange_cmd(&self, writer: &mut WriteHalf<'_>) -> anyhow::Result<()> {
        let XRange {
            stream_key,
            start,
            end,
        } = self
        else {
            bail!("Not a xrange cmd");
        };
        let (tx, rx) = oneshot::channel::<Vec<StreamDbValueType>>();
        Database::emit(DatabaseEvent::XRange {
            resp: tx,
            stream_key: stream_key.clone(),
            start: start.clone(),
            end: end.clone(),
        })
        .await?;

        let db_value = rx.await?;
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
        writer.write_all(&final_resp.as_bytes()).await?;
        writer.flush().await?;
        Ok(())
    }

    async fn process_wait_cmd(&self, writer: &mut WriteHalf<'_>) -> anyhow::Result<()> {
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
            writer.write_all(&resp_type.as_bytes()).await?;
            return Ok(());
        }

        let (db_event_resp_emitter, db_event_resp_listener) = oneshot::channel::<bool>();
        Database::emit(DatabaseEvent::WasLastCommandSet {
            resp: db_event_resp_emitter,
        })
        .await?;
        let was_last_command_set = db_event_resp_listener.await?;

        if was_last_command_set == false {
            let resp_type = RESPType::Integer(num_replicas as i64);
            writer.write_all(&resp_type.as_bytes()).await?;
            return Ok(());
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
        writer.write_all(&resp_type.as_bytes()).await?;
        Ok(())
    }
}

async fn send_rds_file(writer: &mut WriteHalf<'_>) -> anyhow::Result<()> {
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
