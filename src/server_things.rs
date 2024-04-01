use anyhow::Context;
use std::{
    collections::HashMap,
    io::Write,
    net::TcpStream,
    sync::{Arc, Mutex},
};
use tracing::{debug, info};

use crate::{
    cli_args::CliArgs, cmd_processor, command::Command, fdbg, master_things, resp_parser::DataType,
    store::Store,
};

pub fn parse_tcp_stream(
    mut stream: TcpStream,
    map: Arc<Store>,
    cmd_args: Arc<HashMap<String, CliArgs>>,
    replicas: Arc<Mutex<Vec<TcpStream>>>,
) -> anyhow::Result<()> {
    loop {
        {
            // Test to check message format
            // use std::io::Read;
            // let mut buf = [0; 256];
            // stream.read(&mut buf)?;
            // info!("Content: {:?}", std::str::from_utf8(&buf).unwrap());
            // anyhow::bail!("^^^^_________ message node received");
        }
        let mut reader = std::io::BufReader::new(&stream);
        let command = Command::parse_with_reader(&mut reader)?;
        let msg = match cmd_processor::process_cmd(
            &command,
            &stream,
            &map,
            &cmd_args,
            Some(&replicas),
        )? {
            None | Some(DataType::EmptyString) => break,
            Some(DataType::NewLine(_)) => continue,
            Some(msg) => msg,
        };
        info!("Response - {msg:?}");
        stream
            .write_all(&msg.as_bytes())
            .context(fdbg!("Unable to write to TcpStream"))?;
        {
            let (command, map, stream, replicas) = (
                command.clone(),
                map.clone(),
                stream.try_clone()?,
                replicas.clone(),
            );
            std::thread::spawn(move || {
                master_things::do_follow_up_if_needed(command, map, stream, replicas)
                    .expect("Follow up should have been successful");
                debug!("Follow up finished");
            });
        }
    }
    Ok(())
}
