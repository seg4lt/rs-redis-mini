use std::collections::HashMap;

use anyhow::bail;

use crate::{fdbg, resp_type::RESPType};

type R = anyhow::Result<ClientCmd>;

#[derive(Debug)]
pub enum ClientCmd {
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
    CustomNewLine,
    ExitConn,
}

impl ClientCmd {
    pub fn from_resp_type(resp_type: &RESPType) -> anyhow::Result<Self> {
        match resp_type {
            RESPType::Array(items) => parse_client_cmd(&items),
            RESPType::CustomNewLine => Ok(ClientCmd::CustomNewLine),
            RESPType::EOF => Ok(ClientCmd::ExitConn),
            _ => bail!("Client command must be of type array"),
        }
    }
}

fn parse_client_cmd(items: &[RESPType]) -> R {
    if items.is_empty() {
        bail!("Client command array must have at least one element");
    }
    let Some(RESPType::BulkString(cmd)) = items.get(0) else {
        bail!("First element of client command array must be a bulk string");
    };
    let cmd = cmd.to_uppercase();
    match cmd.as_str() {
        "PING" => Ok(ClientCmd::Ping),
        "ECHO" => parse_echo_cmd(&items[1..]),
        "SET" => parse_set_cmd(&items[1..]),
        "GET" => parse_get_cmd(&items[1..]),
        "INFO" => parse_info_cmd(&items[1..]),
        "REPLCONF" => parse_replication_conf_cmd(&items[1..]),
        "PSYNC" => parse_psync_cmd(&items[1..]),
        _ => bail!("Unknown client command: {}", cmd),
    }
}
fn parse_psync_cmd(items: &[RESPType]) -> R {
    let Some(RESPType::BulkString(key)) = items.get(0) else {
        bail!(fdbg!("PSYNC command must have at least one key"));
    };
    let Some(RESPType::BulkString(value)) = items.get(1) else {
        bail!(fdbg!("PSYNC command must have at least one value"));
    };
    Ok(ClientCmd::Psync {
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
    Ok(ClientCmd::ReplConf {
        key: key.to_string(),
        value: value.to_string(),
    })
}

fn parse_info_cmd(items: &[RESPType]) -> R {
    let Some(RESPType::BulkString(key)) = items.get(0) else {
        bail!(fdbg!("INFO command must have at least one key"));
    };
    Ok(ClientCmd::Info {
        key: key.to_string(),
    })
}

fn parse_get_cmd(items: &[RESPType]) -> R {
    let Some(RESPType::BulkString(key)) = items.get(0) else {
        bail!(fdbg!("GEt command must have at least key"));
    };
    Ok(ClientCmd::Get {
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
    Ok(ClientCmd::Set {
        key: key.to_owned(),
        value: value.to_owned(),
        flags,
    })
}

fn parse_echo_cmd(items: &[RESPType]) -> R {
    let Some(RESPType::BulkString(value)) = items.get(0) else {
        bail!(fdbg!("ECHO command must have at least one argument"));
    };
    Ok(ClientCmd::Echo(value.to_owned()))
}
