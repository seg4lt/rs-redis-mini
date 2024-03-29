use crate::{command::Command, resp_parser::DataType, store::Store};
use std::{
    io::Write,
    net::{TcpListener, TcpStream},
    sync::{Arc, RwLock},
};

use anyhow::Context;
pub(crate) mod command;
pub(crate) mod resp_parser;
pub(crate) mod store;

pub const LINE_ENDING: &str = "\r\n";
pub const NEW_LINE: u8 = b'\n';

#[test]
fn test() {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .unwrap();
    let test = std::time::Duration::from_millis(10);
    let fut = now + test;
    println!(
        "now: {:?}, test: {:?}, fut: {:?}",
        now.as_millis(),
        test.as_millis(),
        fut.as_millis()
    )
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("Logs from your program will appear here!");

    let shared_map: Arc<RwLock<Store>> = Arc::new(RwLock::new(Store::new()));

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    for stream in listener.incoming() {
        let cloned_map = shared_map.clone();
        // TODO: Implement event loop like redis??
        std::thread::spawn(move || {
            let stream = stream.unwrap();
            parse_tcp_stream(stream, cloned_map)
                .context("Unable to parse tcp stream")
                .unwrap();
        });
    }
    Ok(())
}

fn parse_tcp_stream(mut stream: TcpStream, shared_map: Arc<RwLock<Store>>) -> anyhow::Result<()> {
    loop {
        {
            // Test to check message format
            // let mut buf = [0; 256];
            // stream.read(&mut buf)?;
            // println!("Content: {:?}", std::str::from_utf8(&buf).unwrap());
        }
        let mut reader = std::io::BufReader::new(&stream);
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
            Command::Noop => {
                // Do nothing
                break;
            }
            #[allow(unreachable_patterns)]
            _ => Err(anyhow::anyhow!("Unknown command"))?,
        }
        .to_string();
        stream
            .write_all(msg.as_bytes())
            .context("Unable to write to TcpStream")?;
    }
    Ok(())
}
