use tokio::{io::AsyncWriteExt, net::tcp::WriteHalf};
use tracing::debug;

use crate::{cmd_parser::slave_cmd::SlaveCmd, kvstore::KvChan, resp_type::RESPType, KvStoreCmd};
use SlaveCmd::*;

impl SlaveCmd {
    pub async fn process_slave_cmd(
        &self,
        writer: &mut WriteHalf<'_>,
        kv_chan: &KvChan,
        bytes_received: usize,
    ) -> anyhow::Result<()> {
        match self {
            Ping => {
                let resp_type = RESPType::SimpleString("PONG".to_string());
                // writer.write_all(&resp_type.as_bytes()).await?;
            }
            Set { key, value, flags } => {
                let kv_cmd = KvStoreCmd::Set {
                    key: key.clone(),
                    value: value.clone(),
                    flags: flags.clone(),
                };
                kv_chan.send(kv_cmd).await?;
            }
            ReplConf { .. } => {
                let resp_type = RESPType::Array(vec![
                    RESPType::BulkString("REPLCONF".to_string()),
                    RESPType::BulkString("ACK".to_string()),
                    RESPType::BulkString(format!("{}", bytes_received)),
                ]);
                let content = String::from_utf8(resp_type.as_bytes())?;
                debug!("REpl conf content = {content:?}");
                writer.write_all(&resp_type.as_bytes()).await?;
            }
        };
        Ok(())
    }
}
