use std::time::Duration;

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
                let (tx, rx) = oneshot::channel::<Option<String>>();
                let kv_cmd = DatabaseEvent::Get {
                    resp: tx,
                    key: key.to_owned(),
                };
                Database::emit(kv_cmd).await?;
                match rx.await? {
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
            Wait {
                num_replicas: min_acks_wanted,
                timeout_ms,
            } => {
                let (tx, rx) = oneshot::channel::<usize>();
                ReplicationEvent::GetNumOfReplicas { resp: tx }
                    .emit()
                    .await?;
                let num_replicas = rx.await?;

                match num_replicas {
                    0 => {
                        let resp_type = RESPType::Integer(num_replicas as i64);
                        writer.write_all(&resp_type.as_bytes()).await?;
                        return Ok(());
                    }
                    _ => {
                        let (tx, rx) = oneshot::channel::<bool>();
                        Database::emit(DatabaseEvent::WasLastCommandSet { resp: tx }).await?;
                        let was_last_command_set = rx.await?;
                        match was_last_command_set {
                            false => {
                                let resp_type = RESPType::Integer(num_replicas as i64);
                                writer.write_all(&resp_type.as_bytes()).await?;
                                return Ok(());
                            }
                            true => {
                                let (tx, rx) = oneshot::channel::<usize>();
                                debug!("(inside of wait): Emitting ReplicationEvent::GetAck");
                                ReplicationEvent::GetAck {
                                    min_ack: *min_acks_wanted,
                                    resp: tx,
                                }
                                .emit()
                                .await
                                .expect("Unable to send GETACK");
                                let timeout_ms = timeout_ms * 4;
                                match tokio::time::timeout(
                                    Duration::from_millis(timeout_ms as u64),
                                    rx,
                                )
                                .await
                                {
                                    Ok(Ok(value)) => {
                                        let acks_received = value;
                                        let resp_type = RESPType::Integer(acks_received as i64);
                                        let xx = String::from_utf8(resp_type.as_bytes()).unwrap();
                                        debug!("✅ What did I receive: {}", xx);
                                        let response =
                                            writer.write_all(&resp_type.as_bytes()).await;
                                        debug!("Done with writing. Response - {:?}", response);
                                    }
                                    _ => {
                                        debug!("TIMEOUT or error")
                                    }
                                }
                            }
                        }
                    }
                }
            }
            CustomNewLine | ExitConn => {}
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
