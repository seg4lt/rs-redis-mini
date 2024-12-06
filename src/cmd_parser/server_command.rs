use std::collections::HashMap;

use anyhow::bail;
use tracing::debug;

use crate::{fdbg, resp_type::RESPType};

type R = anyhow::Result<ServerCommand>;

#[derive(Debug, Clone)]
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
        #[allow(dead_code)]
        key: String,
    },
    ReplConf {
        key: String,
        value: String,
    },
    PSync {
        #[allow(dead_code)]
        key: String,
        #[allow(dead_code)]
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
    Type(String),
    XAdd {
        stream_key: String,
        stream_id: String,
        key: String,
        value: String,
    },
    XRange {
        stream_key: String,
        start: String,
        end: String,
    },
    XRead(Vec<(String, String)>, Option<u64>),
    Incr {
        key: String,
    },
    Multi,
    Exec,
    Discard,
    CustomNewLine,
    ExitConn,
}

impl ServerCommand {
    pub fn from(resp_type: &RESPType) -> anyhow::Result<Self> {
        match resp_type {
            RESPType::Array(items) => parse_client_cmd(&items),
            RESPType::CustomNewLine => Ok(ServerCommand::CustomNewLine),
            RESPType::EOF => Ok(ServerCommand::ExitConn),
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
        "PING" => Ok(ServerCommand::Ping),
        "ECHO" => parse_echo_cmd(&items[1..]),
        "SET" => parse_set_cmd(&items[1..]),
        "GET" => parse_get_cmd(&items[1..]),
        "INFO" => parse_info_cmd(&items[1..]),
        "REPLCONF" => parse_replication_conf_cmd(&items[1..]),
        "PSYNC" => parse_psync_cmd(&items[1..]),
        "WAIT" => parse_wait_cmd(&items[1..]),
        "CONFIG" => parse_config_cmd(&items[1..]),
        "KEYS" => parse_keys_cmd(&items[1..]),
        "TYPE" => parse_type_cmd(&items[1..]),
        "XADD" => parse_xadd_cmd(&items[1..]),
        "XRANGE" => parse_xrange_cmd(&items[1..]),
        "XREAD" => parse_xread_cmd(&items[1..]),
        "INCR" => parse_incr_cmd(&items[1..]),
        "MULTI" => parse_multi_cmd(),
        "EXEC" => parse_exec_cmd(),
        "DISCARD" => parse_discard_cmd(),
        _ => bail!("Unknown client command: {}", cmd),
    }
}

fn parse_discard_cmd() -> R {
    Ok(ServerCommand::Discard)
}
fn parse_exec_cmd() -> R {
    Ok(ServerCommand::Exec)
}

fn parse_multi_cmd() -> R {
    Ok(ServerCommand::Multi)
}

fn parse_xread_cmd(items: &[RESPType]) -> R {
    let Some(RESPType::BulkString(typez)) = items.get(0) else {
        bail!(fdbg!("XREAD must have type"));
    };

    let mut block_ms: Option<u64> = None;
    let consumed;

    match typez.to_lowercase().as_str() {
        "block" => {
            let Some(RESPType::BulkString(block)) = items.get(1) else {
                bail!(fdbg!("XREAD must have block"));
            };
            block_ms = Some(block.parse::<u64>()?);
            let Some(RESPType::BulkString(typez)) = items.get(2) else {
                bail!(fdbg!("XREAD must have type"));
            };
            if typez.to_lowercase() != "streams" {
                bail!(fdbg!("XREAD must have type 'streams'"))
            }
            consumed = 3;
        }
        "streams" => {
            consumed = 1;
        }
        _ => bail!(fdbg!("XREAD must have type 'streams' or 'block'")),
    }

    let items = &items[consumed..];
    let len = items.len() / 2;
    let mut filter: Vec<(String, String)> = Vec::with_capacity(len);
    for i in 0..len {
        let RESPType::BulkString(key) = items[i].clone() else {
            bail!(fdbg!("XREAD must have key"));
        };
        let RESPType::BulkString(stream_id) = items[i + len].clone() else {
            bail!(fdbg!("XREAD must have stream_id"));
        };
        filter.push((key.clone(), stream_id.clone()));
    }
    debug!(?filter, ?block_ms, "This is the parsed filter");
    Ok(ServerCommand::XRead(filter, block_ms))
}

fn parse_xrange_cmd(items: &[RESPType]) -> R {
    let Some(RESPType::BulkString(stream_key)) = items.get(0) else {
        bail!(fdbg!("XRANGE must have stream_key"));
    };
    let Some(RESPType::BulkString(start)) = items.get(1) else {
        bail!(fdbg!("XRANGE must have start"));
    };
    let Some(RESPType::BulkString(end)) = items.get(2) else {
        bail!(fdbg!("XRANGE must have end"));
    };
    Ok(ServerCommand::XRange {
        stream_key: stream_key.to_string(),
        start: start.to_string(),
        end: end.to_string(),
    })
}

fn parse_xadd_cmd(items: &[RESPType]) -> R {
    let Some(RESPType::BulkString(stream_key)) = items.get(0) else {
        bail!(fdbg!("XADD must have stream_key"));
    };
    let Some(RESPType::BulkString(stream_id)) = items.get(1) else {
        bail!(fdbg!("XADD must have stream_id"));
    };
    let Some(RESPType::BulkString(key)) = items.get(2) else {
        bail!(fdbg!("XADD must have key"));
    };
    let Some(RESPType::BulkString(value)) = items.get(3) else {
        bail!(fdbg!("XADD must have value"));
    };
    Ok(ServerCommand::XAdd {
        stream_key: stream_key.to_string(),
        stream_id: stream_id.to_string(),
        key: key.to_string(),
        value: value.to_string(),
    })
}

fn parse_type_cmd(items: &[RESPType]) -> R {
    let Some(RESPType::BulkString(value)) = items.get(0) else {
        bail!(fdbg!("TYPE command must have at least one key"));
    };
    Ok(ServerCommand::Type(value.to_string()))
}

fn parse_keys_cmd(items: &[RESPType]) -> R {
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
    Ok(ServerCommand::PSync {
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

fn parse_incr_cmd(items: &[RESPType]) -> R {
    let Some(RESPType::BulkString(key)) = items.get(0) else {
        bail!(fdbg!("Get command must have at least key"));
    };
    Ok(ServerCommand::Incr {
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
