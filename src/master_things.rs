use std::{
    collections::HashMap,
    io::Write,
    net::TcpStream,
    sync::{Arc, Mutex, RwLock},
};

use crate::{
    command::Command,
    resp_parser::DataType,
    store::{Store, KEY_IS_MASTER},
};
use base64::prelude::*;

pub fn do_follow_up_if_needed(
    command: &Command,
    map: &Arc<RwLock<Store>>,
    mut current_stream: &mut TcpStream,
    replicas: &Arc<Mutex<Vec<TcpStream>>>,
) -> anyhow::Result<()> {
    let value = map.write().unwrap().get(KEY_IS_MASTER.into());

    if value.is_none() {
        return Ok(());
    }
    if value.is_some() && value.unwrap() != "true" {
        return Ok(());
    }
    let mut replicas = replicas.lock().unwrap();
    if replicas.len() == 0 {
        return Ok(());
    }
    match command {
        Command::PSync(_, _) => send_rdb_to_replica(&mut current_stream)?,
        Command::Set(key, value, flags) => {
            for mut stream in replicas.iter_mut() {
                broadcast_set_cmd(&mut stream, key, value, flags)?
            }
        }
        _ => {}
    }
    Ok(())
}

fn send_rdb_to_replica(stream: &mut TcpStream) -> anyhow::Result<()> {
    let base64 = b"UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==";
    let decoded_base64 = BASE64_STANDARD.decode(base64).unwrap();
    println!("🙏 >>> Sending RDB to replica: {:?}", decoded_base64.len());
    let d_type = DataType::NotBulkString(decoded_base64);
    stream.write_all(&d_type.as_bytes())?;
    Ok(())
}

fn broadcast_set_cmd(
    stream: &mut TcpStream,
    key: &String,
    value: &String,
    flags: &Option<HashMap<String, String>>,
) -> anyhow::Result<()> {
    let mut items = vec![
        DataType::BulkString("SET".into()),
        DataType::BulkString(key.into()),
        DataType::BulkString(value.into()),
    ];
    if let Some(flags) = flags {
        flags.iter().for_each(|(key, value)| match key.as_str() {
            key if key == "get" && value == "true" => {
                items.push(DataType::BulkString("GET".into()))
            }
            "ex" | "px" => {
                let flag = if key == "ex" {
                    format!("EX{}", value)
                } else {
                    format!("PX{}", value)
                };
                items.push(DataType::BulkString(flag));
            }
            _ => {
                todo!("SET doesn't understand the flag yet")
            }
        });
    }
    let d_type = DataType::Array(items);
    println!(
        "🙏 >>> Sending SET to replica: {:?}",
        String::from_utf8(d_type.as_bytes())
    );
    stream.write_all(&d_type.as_bytes())?;
    Ok(())
}
