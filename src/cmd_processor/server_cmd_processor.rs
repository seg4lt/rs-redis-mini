use crate::{cmd::client_cmd::ClientCmd, resp_type::RESPType};

impl ClientCmd {
    pub fn process_client_cmd(&self) -> anyhow::Result<RESPType> {
        let response_resp_type = match self {
            ClientCmd::Ping => RESPType::SimpleString("PONG".to_string()),
        };
        Ok(response_resp_type)
    }
}
