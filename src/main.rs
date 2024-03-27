use std::{
    io::{BufRead, BufReader, Read, Write},
    net::{TcpListener, TcpStream},
};

use anyhow::Context;

const LINE_ENDING: &str = "\r\n";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    for stream in listener.incoming() {
        std::thread::spawn(move || {
            let stream = stream.unwrap();
            parse_tcp_stream(stream)
                .context("Unable to parse tcp stream")
                .unwrap();
        });
        // let stream = stream.context("Error while accepting connection")?;
        // parse_tcp_stream(stream).context("Unable to parse tcp stream")?;
    }
    Ok(())
}

fn parse_tcp_stream(mut stream: TcpStream) -> anyhow::Result<()> {
    println!("accepted new connection");
    let mut buf = [0; 512];
    loop {
        let read_count = stream.read(&mut buf)?;
        if read_count == 0 {
            break;
        }
        stream
            .write_all(format!("+PONG{LINE_ENDING}").as_bytes())
            .unwrap();
    }
    Ok(())
}
