use std::collections::HashMap;

use anyhow::bail;

use crate::{fdbg, rds_file::parse_rdb_file, resp_type::RESPType};

type R = anyhow::Result<ServerCommand>;

#[derive(Debug)]
pub enum ServerCommand {
    Ping,
    Echo(String),
    Get {
        key: String,
    },
    Set {
        key: String,
        value: String,
        flags: HashMap<String, String>,
    },
    Info {
        key: String,
    },
    ReplConf {
        key: String,
        value: String,
    },
    Psync {
        key: String,
        value: String,
    },
    Wait {
        ack_wanted: usize,
        timeout_ms: usize,
    },
    Config {
        cmd: String,
        key: String,
    },
    Keys(String),
    CustomNewLine,
    ExitConn,
}

impl ServerCommand {
    pub async fn from(resp_type: &RESPType) -> anyhow::Result<Self> {
        match resp_type {
            RESPType::Array(items) => parse_client_cmd(&items).await,
            RESPType::CustomNewLine => Ok(ServerCommand::CustomNewLine),
            RESPType::EOF => Ok(ServerCommand::ExitConn),
            _ => bail!("Client command must be of type array"),
        }
    }
}

async fn parse_client_cmd(items: &[RESPType]) -> R {
    if items.is_empty() {
        bail!("Client command array must have at least one element");
    }
    let Some(RESPType::BulkString(cmd)) = items.get(0) else {
        bail!("First element of client command array must be a bulk string");
    };
    let cmd = cmd.to_uppercase();
    match cmd.as_str() {
        "PING" => Ok(ServerCommand::Ping),
        "ECHO" => parse_echo_cmd(&items[1..]),
        "SET" => parse_set_cmd(&items[1..]),
        "GET" => parse_get_cmd(&items[1..]),
        "INFO" => parse_info_cmd(&items[1..]),
        "REPLCONF" => parse_replication_conf_cmd(&items[1..]),
        "PSYNC" => parse_psync_cmd(&items[1..]),
        "WAIT" => parse_wait_cmd(&items[1..]),
        "CONFIG" => parse_config_cmd(&items[1..]),
        "KEYS" => parse_keys_cmd(&items[1..]).await,
        _ => bail!("Unknown client command: {}", cmd),
    }
}

async fn parse_keys_cmd(items: &[RESPType]) -> R {
    let Some(RESPType::BulkString(value)) = items.get(0) else {
        bail!(fdbg!("KEYS command must have at least one key"));
    };
    Ok(ServerCommand::Keys(value.to_string()))
}

fn parse_config_cmd(items: &[RESPType]) -> R {
    let Some(RESPType::BulkString(cmd)) = items.get(0) else {
        bail!(fdbg!("CONFIG command must have associated command"));
    };
    let Some(RESPType::BulkString(key)) = items.get(1) else {
        bail!(fdbg!("CONFIG command must have at key"));
    };
    Ok(ServerCommand::Config {
        cmd: cmd.to_string(),
        key: key.to_string(),
    })
}

fn parse_wait_cmd(items: &[RESPType]) -> R {
    let Some(RESPType::BulkString(num_replicas)) = items.get(0) else {
        bail!(fdbg!("WAIT command must have at least one key"));
    };
    let Some(RESPType::BulkString(value)) = items.get(1) else {
        bail!(fdbg!("WAIT command must have at least one value"));
    };
    let num_replicas = num_replicas.parse::<usize>()?;
    Ok(ServerCommand::Wait {
        ack_wanted: num_replicas,
        timeout_ms: value.parse::<usize>()?,
    })
}

fn parse_psync_cmd(items: &[RESPType]) -> R {
    let Some(RESPType::BulkString(key)) = items.get(0) else {
        bail!(fdbg!("PSYNC command must have at least one key"));
    };
    let Some(RESPType::BulkString(value)) = items.get(1) else {
        bail!(fdbg!("PSYNC command must have at least one value"));
    };
    Ok(ServerCommand::Psync {
        key: key.to_string(),
        value: value.to_string(),
    })
}
fn parse_replication_conf_cmd(items: &[RESPType]) -> R {
    let Some(RESPType::BulkString(key)) = items.get(0) else {
        bail!(fdbg!("REPLCONF command must have at least one key"));
    };
    let Some(RESPType::BulkString(value)) = items.get(1) else {
        bail!(fdbg!("REPLCONF command must have at least one value"));
    };
    Ok(ServerCommand::ReplConf {
        key: key.to_string(),
        value: value.to_string(),
    })
}

fn parse_info_cmd(items: &[RESPType]) -> R {
    let Some(RESPType::BulkString(key)) = items.get(0) else {
        bail!(fdbg!("INFO command must have at least one key"));
    };
    Ok(ServerCommand::Info {
        key: key.to_string(),
    })
}

fn parse_get_cmd(items: &[RESPType]) -> R {
    let Some(RESPType::BulkString(key)) = items.get(0) else {
        bail!(fdbg!("GEt command must have at least key"));
    };
    Ok(ServerCommand::Get {
        key: key.to_owned(),
    })
}

fn parse_set_cmd(items: &[RESPType]) -> R {
    let Some(RESPType::BulkString(key)) = items.get(0) else {
        bail!(fdbg!("SET command must have at least key"));
    };
    let Some(RESPType::BulkString(value)) = items.get(1) else {
        bail!(fdbg!("SET command must have at least value"));
    };
    let mut remaining = items[2..].iter();
    let mut flags: HashMap<String, String> = HashMap::new();
    while let Some(flag) = remaining.next() {
        let RESPType::BulkString(flag) = flag else {
            break;
        };
        let flag = flag.to_lowercase();
        match flag.as_str() {
            "px" | "ex" => {
                let Some(RESPType::BulkString(value)) = remaining.next() else {
                    bail!(fdbg!("Missing value for flag: {}", flag));
                };
                flags.insert(flag, value.to_owned());
            }
            "get" => {
                flags.insert(flag, "true".to_owned());
            }
            _ => {}
        }
    }
    Ok(ServerCommand::Set {
        key: key.to_owned(),
        value: value.to_owned(),
        flags,
    })
}

fn parse_echo_cmd(items: &[RESPType]) -> R {
    let Some(RESPType::BulkString(value)) = items.get(0) else {
        bail!(fdbg!("ECHO command must have at least one argument"));
    };
    Ok(ServerCommand::Echo(value.to_owned()))
}
