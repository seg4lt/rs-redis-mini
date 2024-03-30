use anyhow::{anyhow, Context};
use std::{
    collections::HashMap,
    io::Write,
    net::TcpStream,
    sync::{Arc, Mutex, RwLock},
    time::Duration,
};

use crate::{
    cli_args::CliArgs,
    command::Command,
    master_things,
    resp_parser::DataType,
    store::{Store, KEY_IS_MASTER, KEY_MASTER_REPL_OFFSET, KEY_REPLICA_PORT},
};
use crate::{store::KEY_MASTER_REPLID, LINE_ENDING};

pub fn parse_tcp_stream(
    mut stream: TcpStream,
    map: Arc<RwLock<Store>>,
    cmd_args: Arc<HashMap<String, CliArgs>>,
    replicas: Arc<Mutex<Vec<TcpStream>>>,
) -> anyhow::Result<()> {
    loop {
        {
            // Test to check message format
            // use std::io::Read;
            // let mut buf = [0; 256];
            // stream.read(&mut buf)?;
            // println!("Content: {:?}", std::str::from_utf8(&buf).unwrap());
            // anyhow::bail!("^^^^_________ message node received");
        }
        let mut reader = std::io::BufReader::new(&stream);
        let command = Command::parse_with_reader(&mut reader)?;
        let msg = match &command {
            Command::Ping(_) => DataType::SimpleString("PONG".into()),
            Command::Echo(value) => DataType::SimpleString(value.clone()),
            Command::Set(key, value, flags) => process_set_cmd(&map, key, value, flags)?,
            Command::Get(key) => process_get_cmd(&map, key)?,
            Command::Info(_) => process_info_cmd(&map, &cmd_args),
            Command::ReplConf(option, value) => process_replconf_cmd(option, value, &map)?,
            Command::PSync(_, _) => {
                // Add replica to lis when PSYNC is called
                // replicas.lock().unwrap().push(stream.try_clone()?);
                replicas
                    .lock()
                    .unwrap()
                    .push(stream.try_clone().context("Failed to clone the stream")?);
                process_psync_cmd(&map)?
            }
            Command::Noop => {
                println!("🙏 >>> Noop command <<<");
                // Do nothing
                break;
            }
            #[allow(unreachable_patterns)]
            _ => Err(anyhow!("Unknown command - can't do anything"))?,
        };
        println!(
            "🙏 >>> Response: {:?} <<<",
            std::str::from_utf8(&msg.as_bytes()).unwrap()
        );
        stream
            .write_all(&msg.as_bytes())
            .context("Unable to write to TcpStream")?;
        master_things::do_follow_up_if_needed(&command, &map, &replicas)?;
    }
    Ok(())
}

fn process_replconf_cmd(
    option: &String,
    value: &String,
    map: &Arc<RwLock<Store>>,
) -> anyhow::Result<DataType> {
    match option.as_str() {
        "listening-port" => {
            let mut map = map.write().unwrap();
            map.set(KEY_REPLICA_PORT.into(), value.clone(), None);
        }
        _ => {}
    }
    Ok(DataType::SimpleString("OK".to_string()))
}

fn process_psync_cmd(map: &Arc<RwLock<Store>>) -> anyhow::Result<DataType> {
    let mut map = map.write().unwrap();
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

fn process_info_cmd(
    map: &Arc<RwLock<Store>>,
    cmd_args: &Arc<HashMap<String, CliArgs>>,
) -> DataType {
    let is_replica = cmd_args.get("--replicaof").is_some();
    let mut msg = vec![
        format!("# Replication"),
        format!("role:{}", if is_replica { "slave" } else { "master" }),
    ];
    if !is_replica {
        let mut map = map.write().unwrap();
        let master_replid = map.get(KEY_MASTER_REPLID.into()).unwrap();
        let master_repl_offset = map.get(KEY_MASTER_REPL_OFFSET.into()).unwrap_or("0".into());
        msg.push(format!("master_replid:{}", master_replid));
        msg.push(format!("master_repl_offset:{}", master_repl_offset))
    }
    DataType::BulkString(format!("{}{LINE_ENDING}", msg.join(LINE_ENDING)))
}

fn process_get_cmd(map: &Arc<RwLock<Store>>, key: &String) -> anyhow::Result<DataType> {
    // Write lock here because get for now also removes expired keys
    let mut map = map.write().unwrap();
    let msg = match map.get(key.clone()) {
        Some(value) => DataType::BulkString(value.to_string()),
        None => DataType::NullBulkString,
    };
    Ok(msg)
}

fn process_set_cmd(
    map: &Arc<RwLock<Store>>,
    key: &String,
    value: &String,
    flags: &Option<HashMap<String, String>>,
) -> anyhow::Result<DataType> {
    let mut map = map.write().unwrap();

    match flags {
        None => {
            map.set(key.clone(), value.clone(), None);
            Ok(DataType::SimpleString("OK".to_string()))
        }
        Some(flags) => {
            let mut exp_time = None;
            let mut do_get = false;
            for (key, value) in flags.iter() {
                match key.as_str() {
                    "get" => {
                        do_get = true;
                    }
                    "ex" => {
                        let time = value.parse::<u64>().context("Unable to parse time")?;
                        exp_time = Some(Duration::from_secs(time));
                    }
                    "px" => {
                        let time = value.parse::<u64>().context("Unable to parse time")?;
                        exp_time = Some(Duration::from_millis(time));
                    }
                    _ => {
                        todo!("SET doesn't understand the flag yet")
                    }
                }
            }
            let old_value = map.get(key.clone());
            map.set(key.clone(), value.clone(), exp_time);
            match do_get {
                true => match old_value {
                    Some(old_value) => Ok(DataType::BulkString(old_value)),
                    None => Ok(DataType::NullBulkString),
                },
                false => Ok(DataType::SimpleString("OK".to_string())),
            }
        }
    }
}
