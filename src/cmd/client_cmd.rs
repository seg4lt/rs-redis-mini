use anyhow::bail;

use crate::resp_type::RESPType;

pub enum ClientCmd {
    Ping,
}

impl RESPType {
    pub fn to_client_cmd(&self) -> anyhow::Result<ClientCmd> {
        match self {
            RESPType::Array(items) => parse_client_cmd(&items),
            _ => bail!("Client command must be of type array"),
        }
    }
}

fn parse_client_cmd(items: &[RESPType]) -> anyhow::Result<ClientCmd> {
    if items.is_empty() {
        bail!("Client command array must have at least one element");
    }
    let Some(RESPType::BulkString(cmd)) = items.get(0) else {
        bail!("First element of client command array must be a bulk string");
    };
    let cmd = cmd.to_uppercase();
    match cmd.as_str() {
        "PING" => Ok(ClientCmd::Ping),
        _ => bail!("Unknown client command: {}", cmd),
    }
}
