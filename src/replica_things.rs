use std::{
    collections::HashMap,
    io::Write,
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
    let span = span!(Level::INFO, "[Replica]");
    let _guard = span.enter();

    let server = format!("{}:{}", ip, master_port);
    let mut stream = TcpStream::connect(&server).context(fdbg!("Cannot connect to tcp stream"))?;
    debug!("Connected with server = {server}");

    let msg = DataType::Array(vec![DataType::BulkString("PING".into())]);
    info!("{msg:?}");
    stream.write_all(msg.as_bytes().as_ref())?;
    let mut reader = std::io::BufReader::new(&stream);
    let response = DataType::parse(&mut reader)?;
    info!("Response {response:?}");

    let msg = DataType::Array(vec![
        DataType::BulkString("REPLCONF".to_string()),
        DataType::BulkString("listening-port".to_string()),
        DataType::BulkString(format!("{}", port)),
    ]);

    info!("To Master - {msg:?}");
    stream.write_all(msg.as_bytes().as_ref())?;
    let mut reader = std::io::BufReader::new(&stream);
    let response = DataType::parse(&mut reader)?;
    info!("Response - {response:?}");

    let msg = DataType::Array(vec![
        DataType::BulkString("REPLCONF".to_string()),
        DataType::BulkString("capa".to_string()),
        DataType::BulkString("psync2".to_string()),
    ]);

    info!("To Master - {msg:?}");
    stream.write_all(&msg.as_bytes())?;
    let mut reader = std::io::BufReader::new(&stream);
    let response = DataType::parse(&mut reader)?;
    info!("Response - {response:?}");

    let msg = DataType::Array(vec![
        DataType::BulkString("PSYNC".to_string()),
        DataType::BulkString("?".to_string()),
        DataType::BulkString("-1".to_string()),
    ]);

    info!("To Master - {msg:?}");
    stream.write_all(&msg.as_bytes())?;

    let mut reader = std::io::BufReader::new(&stream);
    let response = DataType::parse(&mut reader).context(fdbg!("Expected FULLRESYNC message"))?;
    info!("Response - {response:?}");

    // RDS file
    let mut reader = std::io::BufReader::new(&stream);
    let response = DataType::parse(&mut reader).context(fdbg!("Unable to read RDS content"))?;
    match response {
        DataType::NotBulkString(_) => {}
        d_type => bail!("Did not receive a valid RDS from master - {:?}", d_type),
    }
    loop {
        info!("Waiting for master response");
        let mut reader = std::io::BufReader::new(&stream);
        let cmd = Command::parse_with_reader(&mut reader).context(fdbg!("command parse error"))?;
        let msg = match cmd_processor::process_cmd(&cmd, &stream, &map, &args, None)
            .context(fdbg!("command process error"))
        {
            Ok(msg) => msg,
            Err(e) => {
                warn!("Error processing command {:?}", e);
                break;
            }
        };
        if let Some(DataType::EmptyString) = msg {
            warn!("Received empty string from master");
            break;
        }
    }
    warn!("Connection with master closed");
    Ok(())
}
