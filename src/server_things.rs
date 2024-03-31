use anyhow::Context;
use std::{
    collections::HashMap,
    io::Write,
    net::TcpStream,
    sync::{Arc, Mutex, RwLock},
};

use crate::{
    cli_args::CliArgs, cmd_processor, command::Command, fdbg, master_things, store::Store,
};

pub fn parse_tcp_stream(
    mut stream: TcpStream,
    map: Arc<RwLock<Store>>,
    cmd_args: Arc<HashMap<String, CliArgs>>,
    replicas: Arc<Mutex<Vec<TcpStream>>>,
) -> anyhow::Result<()> {
    loop {
        {
            // Test to check message format
            // use std::io::Read;
            // let mut buf = [0; 256];
            // stream.read(&mut buf)?;
            // println!("Content: {:?}", std::str::from_utf8(&buf).unwrap());
            // anyhow::bail!("^^^^_________ message node received");
        }
        let mut reader = std::io::BufReader::new(&stream);
        let command = Command::parse_with_reader(&mut reader)?;
        let Some(msg) =
            cmd_processor::process_cmd(&command, &stream, &map, &cmd_args, Some(&replicas), true)?
        else {
            break;
        };
        println!(
            "🙏 >>> Response: {:?} <<<",
            std::str::from_utf8(&msg.as_bytes()).unwrap()
        );
        stream
            .write_all(&msg.as_bytes())
            .context(fdbg!("Unable to write to TcpStream"))?;
        master_things::do_follow_up_if_needed(&command, &map, &mut stream, &replicas)?;
    }
    Ok(())
}
