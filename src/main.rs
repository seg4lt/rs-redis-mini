use crate::{command::Command, resp_parser::DataType, store::Store};
use std::{
    collections::HashMap,
    io::Write,
    net::{TcpListener, TcpStream},
    sync::{Arc, RwLock},
};

use anyhow::{anyhow, Context};
use cli_args::CliArgs;
pub(crate) mod cli_args;
pub(crate) mod command;
pub(crate) mod hash;
pub(crate) mod resp_parser;
pub(crate) mod store;

pub const LINE_ENDING: &str = "\r\n";
pub const NEW_LINE: u8 = b'\n';

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("Logs from your program will appear here!");

    let shared_map: Arc<RwLock<Store>> = Arc::new(RwLock::new(Store::new()));
    let cmd_args = Arc::new(CliArgs::get()?);
    let default_port = "6379".to_string();
    let port = match cmd_args.get("--port") {
        Some(CliArgs::Port(port)) => port,
        _ => &default_port,
    };
    match cmd_args.get("--replicaof") {
        None => {
            let hash = hash::generate_random_string();
            shared_map
                .write()
                .unwrap()
                .set("__$$__master_replid".to_string(), hash, None);
        }
        Some(CliArgs::ReplicaOf(ip, master_port)) => sync_with_master(port, ip, master_port)?,
        _ => Err(anyhow!("Invalid --replicaof argument"))?,
    }

    let listener = TcpListener::bind(format!("127.0.0.1:{}", port)).unwrap();
    for stream in listener.incoming() {
        let cloned_map = shared_map.clone();
        let cloned_args = cmd_args.clone();
        // TODO: Implement event loop like redis??
        std::thread::spawn(move || {
            let stream = stream.unwrap();
            parse_tcp_stream(stream, cloned_map, cloned_args)
                .context("Unable to parse tcp stream")
                .unwrap();
        });
    }
    Ok(())
}

fn sync_with_master(port: &String, ip: &String, master_port: &String) -> anyhow::Result<()> {
    let server = format!("{}:{}", ip, master_port);
    let mut stream = TcpStream::connect(server).context("Cannot connect to tcp stream")?;

    // Send PING to master
    let msg = DataType::Array(vec![DataType::BulkString("PING".to_string())]);
    println!("🙏 >>> ToMaster: {:?} <<<", msg.to_string());
    stream.write_all(msg.to_string().as_ref())?;

    let mut reader = std::io::BufReader::new(&stream);
    let response = DataType::parse(&mut reader)?;
    println!("🙏 >>> FromMaster: {:?} <<<", response.to_string());

    // Send REPLCONF listening-port <port>
    let msg = DataType::Array(vec![
        DataType::BulkString("REPLCONF".to_string()),
        DataType::BulkString("listening-port".to_string()),
        DataType::BulkString(format!("{}", port)),
    ]);

    println!("🙏 >>> ToMaster: {:?} <<<", msg.to_string());
    stream.write_all(msg.to_string().as_ref())?;

    // Send REPLCONF capa psync2
    let mut reader = std::io::BufReader::new(&stream);
    let response = DataType::parse(&mut reader)?;
    println!("🙏 >>> FromMaster: {:?} <<<", response.to_string());

    let msg = DataType::Array(vec![
        DataType::BulkString("REPLCONF".to_string()),
        DataType::BulkString("capa".to_string()),
        DataType::BulkString("psync2".to_string()),
    ]);

    println!("🙏 >>> ToMaster: {:?} <<<", msg.to_string());
    stream.write_all(msg.to_string().as_ref())?;

    let mut reader = std::io::BufReader::new(&stream);
    let response = DataType::parse(&mut reader)?;
    println!("🙏 >>> FromMaster: {:?} <<<", response.to_string());

    Ok(())
}

fn parse_tcp_stream(
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
