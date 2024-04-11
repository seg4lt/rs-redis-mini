use std::collections::HashMap;

use anyhow::bail;

use super::server_command::ServerCommand;

/// These are commands that are sent by the master to the slave
#[derive(Debug)]
pub enum SlaveCommand {
    Ping,
    Set {
        key: String,
        value: String,
        flags: HashMap<String, String>,
    },
    ReplConf {
        key: String,
        value: String,
    },
}
impl SlaveCommand {
    // Need to find a better way later
    // Hack for now, because RESPType can be converted to ServerCommand
    pub fn from(client_cmd: &ServerCommand) -> anyhow::Result<Self> {
        match client_cmd {
            ServerCommand::Ping => Ok(SlaveCommand::Ping),
            ServerCommand::Set { key, value, flags } => Ok(SlaveCommand::Set {
                key: key.clone(),
                value: value.clone(),
                flags: flags.clone(),
            }),
            ServerCommand::ReplConf { key, value } => Ok(SlaveCommand::ReplConf {
                key: key.clone(),
                value: value.clone(),
            }),
            _ => bail!("Only SET command is supported for now = {:?}", client_cmd),
        }
    }
}
