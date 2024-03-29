use anyhow::{anyhow, Context};
use std::{
    collections::HashMap,
    io::Write,
    net::TcpStream,
    sync::{Arc, RwLock},
};

use crate::LINE_ENDING;
use crate::{cli_args::CliArgs, command::Command, resp_parser::DataType, store::Store};

pub fn parse_tcp_stream(
    mut stream: TcpStream,
    shared_map: Arc<RwLock<Store>>,
    cmd_args: Arc<HashMap<String, CliArgs>>,
) -> anyhow::Result<()> {
    loop {
        {
            // Test to check message format
            // use std::io::Read;
            // let mut buf = [0; 256];
            // stream.read(&mut buf)?;
            // println!("Content: {:?}", std::str::from_utf8(&buf).unwrap());
        }
        let mut reader: std::io::BufReader<&TcpStream> = std::io::BufReader::new(&stream);
        let msg = match Command::parse_with_reader(&mut reader)? {
            Command::Ping(_) => DataType::SimpleString("PONG".to_string()),
            Command::Echo(value) => DataType::SimpleString(value),
            Command::Set(key, value, do_get, exp_time) => {
                let mut map = shared_map.write().unwrap();
                let old_value = map.get(key.clone());
                map.set(key, value, exp_time);
                match do_get {
                    true => match old_value {
                        Some(old_value) => DataType::BulkString(old_value),
                        None => DataType::NullBulkString,
                    },
                    false => DataType::SimpleString("OK".to_string()),
                }
            }
            Command::Get(key) => {
                // Write lock here because get for now also removes expired keys
                let mut map = shared_map.write().unwrap();
                match map.get(key) {
                    Some(value) => DataType::BulkString(value.to_string()),
                    None => DataType::NullBulkString,
                }
            }
            Command::Info(_) => {
                let is_replica = cmd_args.get("--replicaof").is_some();
                let mut msg = vec![
                    format!("# Replication"),
                    format!("role:{}", if is_replica { "slave" } else { "master" }),
                ];
                if !is_replica {
                    let mut map = shared_map.write().unwrap();
                    let master_replid = map.get("__$$__master_replid".to_string()).unwrap();
                    msg.push(format!("master_replid:{}", master_replid));
                    msg.push(format!("master_repl_offset:{}", "0"))
                }
                DataType::BulkString(format!("{}{LINE_ENDING}", msg.join(LINE_ENDING)))
            }
            Command::ReplConf(_, _) => DataType::SimpleString("OK".to_string()),
            Command::Noop => {
                // Do nothing
                break;
            }
            #[allow(unreachable_patterns)]
            _ => Err(anyhow!("Unknown command"))?,
        }
        .to_string();
        println!("🙏 >>> Response: {:?} <<<", msg);
        stream
            .write_all(msg.as_bytes())
            .context("Unable to write to TcpStream")?;
    }
    Ok(())
}
