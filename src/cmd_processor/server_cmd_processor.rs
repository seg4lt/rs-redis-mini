use tokio::{io::AsyncWriteExt, net::tcp::WriteHalf};

use crate::{cmd::client_cmd::ClientCmd, resp_type::RESPType};

impl ClientCmd {
    pub async fn process_client_cmd(&self, writer: &mut WriteHalf<'_>) -> anyhow::Result<bool> {
        let end_connection = match self {
            ClientCmd::Ping => {
                let resp_type = RESPType::SimpleString("PONG".to_string());
                writer.write_all(&resp_type.as_bytes()).await?;
                false
            }
            ClientCmd::CustomNewLine => false,
            ClientCmd::EOF => true,
        };
        Ok(end_connection)
    }
}
