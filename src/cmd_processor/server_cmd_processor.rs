use tokio::{
    io::AsyncWriteExt,
    net::tcp::WriteHalf,
    sync::{mpsc, oneshot},
};

use crate::{cmd::client_cmd::ClientCmd, resp_type::RESPType, KvStoreCmd};
use ClientCmd::*;

impl ClientCmd {
    pub async fn process_client_cmd(
        &self,
        writer: &mut WriteHalf<'_>,
        kv_chan: &mpsc::Sender<KvStoreCmd>,
    ) -> anyhow::Result<()> {
        match self {
            Ping => {
                let resp_type = RESPType::SimpleString("PONG".to_string());
                writer.write_all(&resp_type.as_bytes()).await?;
            }
            Echo(value) => {
                let resp_type = RESPType::BulkString(value.clone());
                writer.write_all(&resp_type.as_bytes()).await?;
            }
            Set { key, value } => {
                let kv_cmd = KvStoreCmd::Set {
                    key: key.clone(),
                    value: value.clone(),
                };
                kv_chan.send(kv_cmd).await?;
                let resp_type = RESPType::SimpleString("OK".to_string());
                writer.write_all(&resp_type.as_bytes()).await?;
            }
            Get { key } => {
                let (tx, rx) = oneshot::channel::<Option<String>>();
                let kv_cmd = KvStoreCmd::Get {
                    resp: tx,
                    key: key.to_owned(),
                };
                kv_chan.send(kv_cmd).await?;
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
            CustomNewLine | EOF => {}
        };
        Ok(())
    }
}
