use std::{
    collections::HashMap,
    io::Write,
    net::TcpStream,
    sync::{Arc, RwLock},
};

use anyhow::Context;
use tracing::info;

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
    let mut stream = TcpStream::connect(server).context(fdbg!("Cannot connect to tcp stream"))?;

    // Send PING to master
    let msg = DataType::Array(vec![DataType::BulkString("PING".into())]);
    stream.write_all(msg.as_bytes().as_ref())?;
    let mut reader = std::io::BufReader::new(&stream);
    let response = DataType::parse(&mut reader)?;
    info!("🙏 >>> FromMaster: {:?} <<<", response);

    // Send REPLCONF listening-port <port>
    let msg = DataType::Array(vec![
        DataType::BulkString("REPLCONF".to_string()),
        DataType::BulkString("listening-port".to_string()),
        DataType::BulkString(format!("{}", port)),
    ]);

    info!("🙏 >>> ToMaster: {:?} <<<", msg);
    stream.write_all(msg.as_bytes().as_ref())?;
    let mut reader = std::io::BufReader::new(&stream);
    let response = DataType::parse(&mut reader)?;
    info!("🙏 >>> FromMaster: {:?} <<<", response);

    // Send REPLCONF capa psync2
    let msg = DataType::Array(vec![
        DataType::BulkString("REPLCONF".to_string()),
        DataType::BulkString("capa".to_string()),
        DataType::BulkString("psync2".to_string()),
    ]);

    info!("🙏 >>> ToMaster: {:?} <<<", msg);
    stream.write_all(&msg.as_bytes())?;
    let mut reader = std::io::BufReader::new(&stream);
    let response = DataType::parse(&mut reader)?;
    info!("🙏 >>> FromMaster: {:?} <<<", response);

    // Sendc PSYNC <master_replid> <offset>
    let msg = DataType::Array(vec![
        DataType::BulkString("PSYNC".to_string()),
        DataType::BulkString("?".to_string()),
        DataType::BulkString("-1".to_string()),
    ]);

    info!("🙏 >>> ToMaster: {:?} <<<", msg);
    stream.write_all(&msg.as_bytes())?;
    loop {
        info!("🙏 >>> Starting Master Work <<<",);
        let mut reader = std::io::BufReader::new(&stream);
        let cmd = Command::parse_with_reader(&mut reader)
            .context(fdbg!("Replica command parse error"))?;
        let Ok(msg) = cmd_processor::process_cmd(&cmd, &stream, &map, &args, None, false)
            .context(fdbg!("Replica command process error"))
        else {
            break;
        };
        if let Some(DataType::EmptyString) = msg {
            break;
        }
        info!("Received from master {:?}", msg);
        info!("🙏 >>> Finished Master Work <<<",);
    }
    info!("⭕️ >>> Connection with master closed <<<");
    Ok(())
}
