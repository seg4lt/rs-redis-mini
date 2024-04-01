use std::{
    collections::HashMap,
    io::{BufRead, BufReader, Write},
    net::TcpStream,
    sync::Arc,
};

use anyhow::{bail, Context};
use tracing::{debug, info, span, warn, Level};

use crate::{
    cli_args::CliArgs,
    cmd_processor,
    command::Command,
    fdbg,
    resp_parser::DataType,
    store::{Store, KEY_REPLCONF_ACK_OFFSET},
};

pub fn sync_with_master(
    port: String,
    ip: String,
    master_port: String,
    map: Arc<Store>,
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
        let request_dtype = DataType::parse(&mut reader)?;
        let request_dtype_len = request_dtype.as_bytes().len();
        let cmd = Command::parse(request_dtype).context(fdbg!("command parse error"))?;
        let ret_dtype = cmd_processor::process_cmd(&cmd, &stream, &map, &args, None);
        debug!("Received command from master - {cmd:?}");
        if let Ok(_) = ret_dtype {
            let offset = map
                .get(KEY_REPLCONF_ACK_OFFSET.into())
                .map(|v| v.parse::<usize>().unwrap())
                .unwrap_or(0);
            let offset = offset + request_dtype_len;
            map.set(KEY_REPLCONF_ACK_OFFSET.into(), format!("{}", offset), None);
        }
        let ret_dtype = match ret_dtype {
            Ok(Some(dtype)) => dtype,
            Ok(None) => continue,
            Err(e) => {
                warn!("Error processing command {:?}", e);
                break;
            }
        };
        debug!("Response DataType - {ret_dtype:?}");
        let ret_dtype = match ret_dtype {
            DataType::Array(_) => ret_dtype,
            DataType::EmptyString => {
                warn!("Received empty string from master");
                break;
            }
            _ => {
                debug!("Received data type that is not returned to master - {ret_dtype:?}");
                continue;
            }
        };
        info!("Sending response to master - {ret_dtype:?}");
        stream.write_all(&ret_dtype.as_bytes())?
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

    if let DataType::SimpleString(content) =
        DataType::parse(reader).context(fdbg!("Expected FULLRESYNC message"))?
    {
        info!("FULLRESYNC message - {content}");
    } else {
        bail!("Expected FULLRESYNC message");
    }

    // RDS file
    let response =
        DataType::parse_rds_string(reader).context(fdbg!("Unable to read RDS content"))?;
    match response {
        DataType::RDSFile(_) => Ok(()),
        d_type => bail!("Did not receive a valid RDS from master - {:?}", d_type),
    }
}
