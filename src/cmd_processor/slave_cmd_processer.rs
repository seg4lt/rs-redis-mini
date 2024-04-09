use tokio::{io::AsyncWriteExt, net::tcp::WriteHalf, sync::oneshot};
use tracing::debug;

use crate::{cmd_parser::slave_cmd::SlaveCmd, kvstore::KvChan, resp_type::RESPType, KvStoreCmd};
use SlaveCmd::*;

impl SlaveCmd {
    pub async fn process_slave_cmd(
        &self,
        writer: &mut WriteHalf<'_>,
        kv_chan: &KvChan,
    ) -> anyhow::Result<()> {
        match self {
            Set { key, value, flags } => {
                let kv_cmd = KvStoreCmd::Set {
                    key: key.clone(),
                    value: value.clone(),
                    flags: flags.clone(),
                };
                kv_chan.send(kv_cmd).await?;
            }
            ReplConf { key, value } => {
                let resp_type = RESPType::Array(vec![
                    RESPType::BulkString("REPLCONF".to_string()),
                    RESPType::BulkString("ACK".to_string()),
                    RESPType::BulkString("0".to_string()),
                ]);
                writer.write_all(&resp_type.as_bytes()).await?;
            }
        };
        Ok(())
    }
}
