pub(crate) mod process_wait_cmd;

use process_wait_cmd::process_wait_cmd;

use std::{collections::HashMap, io::Write, net::TcpStream, sync::Arc, time::Duration};

use anyhow::{anyhow, bail, Context};
use tracing::{debug, error};

use crate::{
    cli_args::CliArgs,
    command::Command,
    fdbg,
    resp_parser::DataType,
    store::{
        Store, KEY_IS_MASTER, KEY_MASTER_REPLID, KEY_MASTER_REPL_OFFSET, KEY_REPLCONF_ACK_OFFSET,
        KEY_REPLICA_PORT,
    },
    types::Replicas,
    LINE_ENDING,
};

pub fn process_cmd(
    cmd: &Command,
    stream: &TcpStream,
    map: &Arc<Store>,
    cmd_args: &Arc<HashMap<String, CliArgs>>,
    replicas: Option<&Arc<Replicas>>,
) -> anyhow::Result<Option<DataType>> {
    let ret_data_type = match cmd {
        Command::Ping(_) => DataType::SimpleString("PONG".into()),
        Command::Echo(value) => DataType::SimpleString(value.clone()),
        Command::Set(key, value, flags) => process_set_cmd(&map, key, value, flags)?,
        Command::Get(key) => process_get_cmd(&map, key)?,
        Command::Info(_) => process_info_cmd(&map, &cmd_args),
        Command::ReplConf(option, value) => process_replconf_cmd(option, value, &map),
        Command::PSync(_, _) => {
            let ret_cmd = process_psync_cmd(&map)?;
            // Add replica to lis when PSYNC is called
            if let Some(replicas) = replicas {
                // TODO: Remove replicas when they go down
                replicas.add(
                    stream
                        .try_clone()
                        .context(fdbg!("Unable to clone tcp stream to add to cache"))?,
                );
            }
            ret_cmd
        }
        Command::Wait(num_of_replicas, timeout_ms) => {
            process_wait_cmd(*num_of_replicas, *timeout_ms, map, replicas)?
        }
        Command::ConnectionClosed => DataType::EmptyString,
        Command::Noop(_comment) => {
            // info!("Received Noop command - {:?} <<<", comment);
            // Do nothing
            return Ok(None);
        }
        Command::CustomUserCommand(command) => {
            let command = command
                .split(" ")
                .map(|s| s.to_string())
                .collect::<Vec<_>>();
            let mut bulkstring = vec![];
            command.iter().for_each(|s| {
                bulkstring.push(DataType::BulkString(s.clone()));
            });
            let ret_data_type = DataType::Array(bulkstring);

            replicas.and_then(|replicas| {
                let length = replicas.len();
                for i in 0..length {
                    let replica = replicas.get(i).unwrap();
                    let mut replica = replica.try_clone().unwrap();
                    let _ = replica.write_all(&ret_data_type.as_bytes());
                }
                Some(())
            });
            return Ok(None);
        }
    };
    Ok(Some(ret_data_type))
}

fn process_replconf_cmd(option: &String, value: &String, map: &Arc<Store>) -> DataType {
    match option.to_lowercase().as_str() {
        "getack" => {
            let offset = map
                .get(KEY_REPLCONF_ACK_OFFSET.into())
                .map(|v| v.parse::<usize>().unwrap())
                .unwrap_or(0);
            DataType::Array(vec![
                DataType::BulkString("REPLCONF".into()),
                DataType::BulkString("ACK".into()),
                DataType::BulkString(format!("{}", offset)),
            ])
        }
        "listening-port" => {
            map.set(KEY_REPLICA_PORT.into(), value.clone(), None);
            DataType::SimpleString("OK".into())
        }
        "capa" => DataType::SimpleString("OK".into()),
        "ack" => DataType::EmptyString,
        _ => {
            error!("Unknown REPLCONF option: {}", option);
            panic!("THIS SHOULD NOT HAPPEN")
        }
    }
}

fn process_psync_cmd(map: &Arc<Store>) -> anyhow::Result<DataType> {
    map.get(KEY_IS_MASTER.into())
        .ok_or_else(|| anyhow!("Not a master"))?;
    let master_replid = map
        .get(KEY_MASTER_REPLID.into())
        .ok_or_else(|| anyhow!("No master replid found"))?;
    let master_repl_offset = map.get(KEY_MASTER_REPL_OFFSET.into()).unwrap_or("0".into());
    Ok(DataType::SimpleString(format!(
        "FULLRESYNC {} {}",
        master_replid, master_repl_offset
    )))
}

fn process_info_cmd(map: &Arc<Store>, cmd_args: &Arc<HashMap<String, CliArgs>>) -> DataType {
    let is_replica = cmd_args.get("--replicaof").is_some();
    let mut msg = vec![
        format!("# Replication"),
        format!("role:{}", if is_replica { "slave" } else { "master" }),
    ];
    if !is_replica {
        let master_replid = map.get(KEY_MASTER_REPLID.into()).unwrap();
        let master_repl_offset = map.get(KEY_MASTER_REPL_OFFSET.into()).unwrap_or("0".into());
        msg.push(format!("master_replid:{}", master_replid));
        msg.push(format!("master_repl_offset:{}", master_repl_offset))
    }
    DataType::BulkString(format!("{}{LINE_ENDING}", msg.join(LINE_ENDING)))
}

fn process_get_cmd(map: &Arc<Store>, key: &String) -> anyhow::Result<DataType> {
    let msg = match map.get(key.clone()) {
        Some(value) => DataType::BulkString(value.to_string()),
        None => DataType::NullBulkString,
    };
    Ok(msg)
}

fn process_set_cmd(
    map: &Arc<Store>,
    key: &String,
    value: &String,
    flags: &Option<HashMap<String, String>>,
) -> anyhow::Result<DataType> {
    let old_value = map.get(key.clone());
    let mut exp_time = None;
    let mut do_get = false;

    match flags {
        None => {}
        Some(flags) => {
            for (key, value) in flags.iter() {
                match key.as_str() {
                    "get" => {
                        do_get = true;
                    }
                    "ex" => {
                        let time = value
                            .parse::<u64>()
                            .context(fdbg!("Unable to parse time"))?;
                        exp_time = Some(Duration::from_secs(time));
                    }
                    "px" => {
                        let time = value
                            .parse::<u64>()
                            .context(fdbg!("Unable to parse time"))?;
                        exp_time = Some(Duration::from_millis(time));
                    }
                    _ => {
                        unimplemented!("SET doesn't understand the flag yet")
                    }
                }
            }
        }
    };

    debug!("Adding/Upading key: {} with value: {}", key, value);
    map.set(key.clone(), value.clone(), exp_time);
    match do_get {
        true => match old_value {
            Some(old_value) => Ok(DataType::BulkString(old_value)),
            None => Ok(DataType::NullBulkString),
        },
        false => Ok(DataType::SimpleString("OK".to_string())),
    }
}
