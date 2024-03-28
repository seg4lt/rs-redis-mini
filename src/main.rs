use crate::command::Command;
use std::{
    io::Write,
    net::{TcpListener, TcpStream},
};

use anyhow::Context;
pub(crate) mod command;
pub(crate) mod resp_parser;

pub const LINE_ENDING: &str = "\r\n";
pub const NEW_LINE: u8 = b'\n';

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    for stream in listener.incoming() {
        // TODO: Implement event loop like redis??
        std::thread::spawn(move || {
            let stream = stream.unwrap();
            parse_tcp_stream(stream)
                .context("Unable to parse tcp stream")
                .unwrap();
        });
    }
    Ok(())
}

fn parse_tcp_stream(mut stream: TcpStream) -> anyhow::Result<()> {
    loop {
        let mut reader = std::io::BufReader::new(&stream);
        if let Ok(command) = Command::parse_with_reader(&mut reader) {
            let msg = match command {
                Command::Ping(_) => {
                    format!("+PONG{LINE_ENDING}")
                }
                Command::Echo(value) => {
                    format!("+{value}{LINE_ENDING}")
                }
            };
            stream
                .write_all(msg.as_bytes())
                .context("Unable to write to TcpStream")?;
        } else {
            break;
        }
    }
    Ok(())
}
