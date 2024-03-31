use std::{
    collections::HashMap,
    io::Write,
    net::TcpStream,
    sync::{Arc, RwLock},
};

use anyhow::Context;
use tracing::{debug, info, warn};

use crate::{
    cli_args::CliArgs, cmd_processor, command::Command, fdbg, resp_parser::DataType, store::Store,
};

pub fn sync_with_master(
    port: String,
    ip: String,
    master_port: String,
    map: Arc<RwLock<Store>>,
    args: Arc<HashMap<String, CliArgs>>,
) -> anyhow::Result<()> {
    let server = format!("{}:{}", ip, master_port);
    let mut stream = TcpStream::connect(&server).context(fdbg!("Cannot connect to tcp stream"))?;
    debug!("Connected with server = {server}");

    let msg = DataType::Array(vec![DataType::BulkString("PING".into())]);
    info!("[Replica] {msg:?}");
    stream.write_all(msg.as_bytes().as_ref())?;
    let mut reader = std::io::BufReader::new(&stream);
    let response = DataType::parse(&mut reader)?;
    info!("[Replica] Response {response:?}");

    let msg = DataType::Array(vec![
        DataType::BulkString("REPLCONF".to_string()),
        DataType::BulkString("listening-port".to_string()),
        DataType::BulkString(format!("{}", port)),
    ]);

    info!("[Replica] To Master - {msg:?}");
    stream.write_all(msg.as_bytes().as_ref())?;
    let mut reader = std::io::BufReader::new(&stream);
    let response = DataType::parse(&mut reader)?;
    info!("[Replica] Response - {response:?}");

    let msg = DataType::Array(vec![
        DataType::BulkString("REPLCONF".to_string()),
        DataType::BulkString("capa".to_string()),
        DataType::BulkString("psync2".to_string()),
    ]);

    info!("[Replica] To Master - {msg:?}");
    stream.write_all(&msg.as_bytes())?;
    let mut reader = std::io::BufReader::new(&stream);
    let response = DataType::parse(&mut reader)?;
    info!("[Replica] Response - {response:?}");

    let msg = DataType::Array(vec![
        DataType::BulkString("PSYNC".to_string()),
        DataType::BulkString("?".to_string()),
        DataType::BulkString("-1".to_string()),
    ]);

    info!("[Replica] To Master - {msg:?}");
    stream.write_all(&msg.as_bytes())?;
    loop {
        info!("[Replica] Waiting for master response");
        let mut reader = std::io::BufReader::new(&stream);
        let cmd = Command::parse_with_reader(&mut reader)
            .context(fdbg!("[Replica] command parse error"))?;
        let msg = match cmd_processor::process_cmd(&cmd, &stream, &map, &args, None)
            .context(fdbg!("[Replica] command process error"))
        {
            Ok(msg) => msg,
            Err(e) => {
                warn!("[Replica] Error processing command {:?}", e);
                break;
            }
        };
        if let Some(DataType::EmptyString) = msg {
            warn!("[Replica] Received empty string from master");
            break;
        }
        info!("[Replica] Response - {msg:?}");
    }
    warn!("[Replica] Connection with master closed");
    Ok(())
}
