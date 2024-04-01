use std::{collections::HashMap, io::Write, net::TcpStream, sync::Arc};

use crate::{
    command::Command,
    fdbg,
    resp_parser::DataType,
    store::{Store, KEY_IS_MASTER},
    types::Replicas,
};
use base64::prelude::*;
use tracing::{info, span, Level};

pub fn do_follow_up_if_needed(
    command: Command,
    map: Arc<Store>,
    mut current_stream: TcpStream,
    replicas: Arc<Replicas>,
) -> anyhow::Result<()> {
    let span = span!(Level::DEBUG, "[Master]");
    let _guard = span.enter();
    let value = map.get(KEY_IS_MASTER.into());
    if value.is_none() {
        return Ok(());
    }
    if value.is_some() && value.unwrap() != "true" {
        return Ok(());
    }
    let num_of_replicas = replicas.len();
    if num_of_replicas == 0 {
        return Ok(());
    }
    match command {
        Command::PSync(_, _) => send_rdb_to_replica(&mut current_stream)?,
        Command::Set(key, value, flags) => {
            for i in 0..num_of_replicas {
                let mut stream = replicas.get(i).expect(&fdbg!("Replica not found"));
                broadcast_set_cmd(&mut stream, &key, &value, &flags)?;
            }
        }
        _ => {}
    };
    Ok(())
}

fn send_rdb_to_replica(stream: &mut TcpStream) -> anyhow::Result<()> {
    let base64 = b"UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==";
    let decoded_base64 = BASE64_STANDARD.decode(base64).unwrap();
    info!("Sending RDB to replica - Length({})", decoded_base64.len());
    let d_type = DataType::RDSFile(decoded_base64);
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
                items.push(DataType::BulkString(key.into()));
                items.push(DataType::BulkString(value.into()));
            }
            _ => {
                info!("SET doesn't understand the flag yet")
            }
        });
    }
    let d_type = DataType::Array(items);
    info!("Broadcasting SET command - {d_type:?}");
    stream.write_all(&d_type.as_bytes())?;
    Ok(())
}
