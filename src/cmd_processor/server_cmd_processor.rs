use std::time::Duration;

use anyhow::bail;
use tokio::{io::AsyncWriteExt, net::tcp::WriteHalf, sync::oneshot};
use tracing::debug;

use crate::{
    app_config::AppConfig,
    cmd_parser::server_command::ServerCommand,
    database::{Database, DatabaseEvent},
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
                let kv_cmd = DatabaseEvent::Set {
                    key: key.clone(),
                    value: value.clone(),
                    flags: flags.clone(),
                };
                Database::emit(kv_cmd).await?;
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
            CustomNewLine | ExitConn => {}
        };
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

        let (replication_event_resp_emitter, replication_event_resp_listener) =
            oneshot::channel::<usize>();
        debug!("(inside of wait): Emitting ReplicationEvent::GetAck");
        ReplicationEvent::GetAck {
            ack_wanted: *ack_wanted,
            resp: replication_event_resp_emitter,
        }
        .emit()
        .await?;

        match tokio::time::timeout(
            Duration::from_millis(*timeout_ms as u64),
            replication_event_resp_listener,
        )
        .await
        {
            Ok(Ok(value)) => {
                let acks_received = value;
                let resp_type = RESPType::Integer(acks_received as i64);
                let xx = String::from_utf8(resp_type.as_bytes()).unwrap();
                debug!("✅ What did I receive: {}", xx);
                writer.write_all(&resp_type.as_bytes()).await?;
            }
            _ => {
                debug!("⏰ TIMEOUT or error");
                let resp_type = RESPType::Integer(0);
                writer.write_all(&resp_type.as_bytes()).await?;
            }
        };
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
