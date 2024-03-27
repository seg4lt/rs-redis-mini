use std::{
    io::{BufRead, BufReader, Read, Write},
    net::{TcpListener, TcpStream},
    str::FromStr,
};

use anyhow::Context;

const LINE_ENDING: &str = "\r\n";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                println!("accepted new connection");
                let mut reader = BufReader::new(&stream);
                let command = read_command(&mut reader)?;
                match command.as_str() {
                    "ping" | "PING" => {
                        stream
                            .write_all(format!("+PONG{LINE_ENDING}").as_bytes())
                            .unwrap();
                    }
                    _ => {
                        stream
                            .write_all(format!("-ERR unknown command{LINE_ENDING}").as_bytes())
                            .unwrap();
                    }
                }
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
    Ok(())
}

fn read_command(reader: &mut BufReader<&TcpStream>) -> anyhow::Result<String> {
    let mut line = String::new();
    // force read third line for now
    reader
        .read_line(&mut line)
        .context("Unable to read command from stream")?;
    line.clear();
    reader
        .read_line(&mut line)
        .context("Unable to read command from stream")?;
    line.clear();
    reader
        .read_line(&mut line)
        .context("Unable to read command from stream")?;
    Ok(line[0..line.len() - LINE_ENDING.len()].to_string())
}
