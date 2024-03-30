use std::{io::Write, net::TcpStream};

use anyhow::Context;

use crate::resp_parser::DataType;

pub fn sync_with_master(port: String, ip: String, master_port: String) -> anyhow::Result<()> {
    let server = format!("{}:{}", ip, master_port);
    let mut stream = TcpStream::connect(server).context("Cannot connect to tcp stream")?;

    // Send PING to master
    let msg = DataType::Array(vec![DataType::BulkString("PING".into())]);
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
    let mut reader = std::io::BufReader::new(&stream);
    let response = DataType::parse(&mut reader)?;
    println!("🙏 >>> FromMaster: {:?} <<<", response.to_string());

    // Send REPLCONF capa psync2
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

    // Sendc PSYNC <master_replid> <offset>
    let msg = DataType::Array(vec![
        DataType::BulkString("PSYNC".to_string()),
        DataType::BulkString("?".to_string()),
        DataType::BulkString("-1".to_string()),
    ]);

    println!("🙏 >>> ToMaster: {:?} <<<", msg.to_string());
    stream.write_all(msg.to_string().as_ref())?;
    loop {
        let mut reader = std::io::BufReader::new(&stream);
        match DataType::parse(&mut reader) {
            Ok(DataType::NotBulkString(data)) => {
                println!("🙏 >>> FromMaster: NotBulkString {:?} <<<", data.len())
            }
            Err(err) => {
                println!("🙏 >>> ERROR: {:?} <<<", err);
                break;
            }
            Ok(DataType::Noop) => {
                println!("🙏 >>> FromMaster: Noop <<<");
                break;
            }
            Ok(d_type) => {
                println!("🙏 >>> FromMaster: Don't know what to do {:?}<<<", d_type);
                continue;
            }
        }
    }
    println!("⭕️ >>> Connection with master closed <<<");
    Ok(())
}
