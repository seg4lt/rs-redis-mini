use tokio::{io::AsyncWriteExt, net::tcp::WriteHalf, sync::oneshot};
use tracing::debug;

use crate::{cmd_parser::slave_cmd::SlaveCmd, kvstore::KvChan, KvStoreCmd};
use SlaveCmd::*;

impl SlaveCmd {
    pub async fn process_client_cmd(&self, kv_chan: &KvChan) -> anyhow::Result<()> {
        match self {
            Set { key, value, flags } => {
                let kv_cmd = KvStoreCmd::Set {
                    key: key.clone(),
                    value: value.clone(),
                    flags: flags.clone(),
                };
                kv_chan.send(kv_cmd).await?;
            }
        };
        Ok(())
    }
}
