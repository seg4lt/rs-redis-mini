use std::{
    collections::HashMap,
    io::{BufRead, BufReader, Write},
    net::TcpStream,
    sync::{Arc, RwLock},
};

use anyhow::{bail, Context};
use tracing::{debug, info, span, warn, Level};

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
    let span = span!(Level::DEBUG, "[Replica]");
    let _guard = span.enter();

    let server = format!("{}:{}", ip, master_port);
    let mut stream = TcpStream::connect(&server).context(fdbg!("Cannot connect to tcp stream"))?;
    let reader = stream.try_clone()?;
    let mut reader = BufReader::new(&reader);

    debug!("Connected with server = {server}");
    handshake(&mut reader, &mut stream, &port)?;

    loop {
        info!("Waiting for master response");
        let cmd = Command::parse_with_reader(&mut reader).context(fdbg!("command parse error"))?;
        let ret_dtype = cmd_processor::process_cmd(&cmd, &stream, &map, &args, None);
        if let Err(e) = ret_dtype {
            warn!("Error processing command {:?}", e);
            break;
        }
        if let Some(DataType::EmptyString) = ret_dtype.unwrap() {
            warn!("Received empty string from master");
            break;
        }
    }
    warn!("Connection with master closed");
    Ok(())
}

fn handshake<R: BufRead>(
    reader: &mut R,
    stream: &mut TcpStream,
    port: &String,
) -> anyhow::Result<()> {
    // PING
    let msg = DataType::Array(vec![DataType::BulkString("PING".into())]);
    info!("Response {msg:?}");
    stream.write_all(msg.as_bytes().as_ref())?;
    let response = DataType::parse(reader)?;
    info!("Response {response:?}");

    // REPLCONF
    let msg = DataType::Array(vec![
        DataType::BulkString("REPLCONF".to_string()),
        DataType::BulkString("listening-port".to_string()),
        DataType::BulkString(format!("{}", port)),
    ]);
    info!("To Master - {msg:?}");
    stream.write_all(msg.as_bytes().as_ref())?;
    let response = DataType::parse(reader)?;
    info!("Response - {response:?}");

    // REPLCONF capa psync2
    let msg = DataType::Array(vec![
        DataType::BulkString("REPLCONF".to_string()),
        DataType::BulkString("capa".to_string()),
        DataType::BulkString("psync2".to_string()),
    ]);

    info!("To Master - {msg:?}");
    stream.write_all(&msg.as_bytes())?;
    let response = DataType::parse(reader)?;
    info!("Response - {response:?}");

    // PSYNC
    let msg = DataType::Array(vec![
        DataType::BulkString("PSYNC".to_string()),
        DataType::BulkString("?".to_string()),
        DataType::BulkString("-1".to_string()),
    ]);

    info!("To Master - {msg:?}");
    stream.write_all(&msg.as_bytes())?;
    let response = DataType::parse(reader).context(fdbg!("Expected FULLRESYNC message"))?;
    info!("Response - {response:?}");

    // RDS file
    // let response = DataType::parse(reader).context(fdbg!("Unable to read RDS content"))?;
    let response = DataType::parse(reader).context(fdbg!("Unable to read RDS content"))?;
    match response {
        DataType::RDSFile(_) => Ok(()),
        d_type => bail!("Did not receive a valid RDS from master - {:?}", d_type),
    }
}
