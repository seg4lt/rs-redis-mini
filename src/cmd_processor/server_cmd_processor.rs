use tokio::{io::AsyncWriteExt, net::tcp::WriteHalf};

use crate::{cmd::client_cmd::ClientCmd, resp_type::RESPType};
use ClientCmd::*;

impl ClientCmd {
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
            CustomNewLine | EOF => {}
        };
        Ok(())
    }
}
