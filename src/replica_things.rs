use std::{
    collections::HashMap,
    io::Write,
    net::TcpStream,
    sync::{Arc, RwLock},
};

use anyhow::Context;

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
    // println!(
    //     "🙏 >>> ToMaster: {:?} <<<",
    //     std::str::from_utf8(&msg.as_bytes()).unwrap()
    // );
    stream.write_all(msg.as_bytes().as_ref())?;
    let mut reader = std::io::BufReader::new(&stream);
    let response = DataType::parse(&mut reader)?;
    println!("🙏 >>> FromMaster: {:?} <<<", response.as_bytes());

    // Send REPLCONF listening-port <port>
    let msg = DataType::Array(vec![
        DataType::BulkString("REPLCONF".to_string()),
        DataType::BulkString("listening-port".to_string()),
        DataType::BulkString(format!("{}", port)),
    ]);

    println!(
        "🙏 >>> ToMaster: {:?} <<<",
        std::str::from_utf8(&msg.as_bytes()).unwrap()
    );
    stream.write_all(msg.as_bytes().as_ref())?;
    let mut reader = std::io::BufReader::new(&stream);
    let response = DataType::parse(&mut reader)?;
    println!("🙏 >>> FromMaster: {:?} <<<", response.as_bytes());

    // Send REPLCONF capa psync2
    let msg = DataType::Array(vec![
        DataType::BulkString("REPLCONF".to_string()),
        DataType::BulkString("capa".to_string()),
        DataType::BulkString("psync2".to_string()),
    ]);

    println!(
        "🙏 >>> ToMaster: {:?} <<<",
        std::str::from_utf8(&msg.as_bytes()).unwrap()
    );
    stream.write_all(&msg.as_bytes())?;
    let mut reader = std::io::BufReader::new(&stream);
    let response = DataType::parse(&mut reader)?;
    println!("🙏 >>> FromMaster: {:?} <<<", response.as_bytes());

    // Sendc PSYNC <master_replid> <offset>
    let msg = DataType::Array(vec![
        DataType::BulkString("PSYNC".to_string()),
        DataType::BulkString("?".to_string()),
        DataType::BulkString("-1".to_string()),
    ]);

    println!(
        "🙏 >>> ToMaster: {:?} <<<",
        std::str::from_utf8(&msg.as_bytes()).unwrap()
    );
    stream.write_all(&msg.as_bytes())?;
    loop {
        println!("🙏 >>> Starting Master Work   <<<",);
        let mut reader = std::io::BufReader::new(&stream);
        let cmd = Command::parse_with_reader(&mut reader)
            .context(fdbg!("Replica command parse error"))?;
        let Ok(_msg) = cmd_processor::process_cmd(&cmd, &stream, &map, &args, None)
            .context(fdbg!("Replica command process error"))
        else {
            // TODO: Need to break out from this loop when server is down
            break;
        };
        println!("🙏 >>> Finished Master Work   <<<",);
    }
    println!("⭕️ >>> Connection with master closed <<<");
    Ok(())
}
