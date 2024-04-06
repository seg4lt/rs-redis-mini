use std::{
    io::{BufRead, Write},
    net::TcpStream,
    sync::Arc,
    time::Instant,
};

use anyhow::Context;
use tracing::debug;

use crate::{fdbg, resp_parser::DataType, store::Store, types::Replicas};

pub fn process_wait_cmd(
    nacks_wanted: usize,
    timeout_ms: u64,
    map: &Arc<Store>,
    replicas: Option<&Arc<Replicas>>,
) -> anyhow::Result<DataType> {
    let replicas_len = replicas.map(|r| r.len()).unwrap_or(0);
    if replicas_len == 0 {
        debug!("No replicas to wait for. Returning 0 as acks received.");
        return Ok(DataType::Integer(0));
    }
    debug!("Number of replica I have - {replicas_len}");
    let get_ack_command = DataType::Array(vec![
        DataType::BulkString("REPLCONF".into()),
        DataType::BulkString("GETACK".into()),
        DataType::BulkString("*".into()),
    ]);
    let get_ack_command_len = get_ack_command.as_bytes().len();
    let start_time = Instant::now();

    let mut ack_received = 0;
    let mut processed_replicas = 0;
    'main_loop: loop {
        if ack_received >= nacks_wanted {
            break 'main_loop;
        }
        if start_time.elapsed().as_millis() > timeout_ms as u128 {
            break 'main_loop;
        }
        for i in 0..replicas_len {
            if processed_replicas >= replicas_len {
                break 'main_loop;
            }
            processed_replicas += 1;
            let mut stream = replicas
                .unwrap()
                .get(i)
                .context(fdbg!("[{}] Unable to get replica from cache", i))?;
            debug!("Sending get ack to index({})", i);
            let result = stream
                .write_all(&get_ack_command.as_bytes())
                .context(fdbg!("[{}] Unable to write to stream for get ack", i));
            debug!("Reponse from write_all. {result:?}");
            let (cmd, flag, offset) = run_get_ack(stream)?;
            debug!("reading get ack to index({})", i);
            debug!(
                "[{}] Received ack from replica: {} {} {}",
                i, cmd, flag, offset
            );
            ack_received += 1;
            if ack_received >= nacks_wanted {
                break 'main_loop;
            }
            if start_time.elapsed().as_millis() > timeout_ms as u128 {
                break 'main_loop;
            }
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    return Ok(DataType::Integer(ack_received as u64));
}

fn run_get_ack(mut stream: TcpStream) -> anyhow::Result<(String, String, usize)> {
    let get_ack_msg = DataType::Array(vec![
        DataType::BulkString("REPLCONF".into()),
        DataType::BulkString("GETACK".into()),
        DataType::BulkString("*".into()),
    ]);
    debug!("Sending get ack to replica. {get_ack_msg:?}");
    stream
        .write_all(&get_ack_msg.as_bytes())
        .context(fdbg!("Unable to write to stream for get ack"))?;
    let mut reader = std::io::BufReader::new(&stream);
    debug!("Trying to read ack reply from replica");
    let len = reader.fill_buf()?;
    debug!("Length of buffer. {:?}", len.len());
    loop {
        if let Ok(d) = DataType::parse_replconf_ack_offset(&mut reader) {
            return Ok(d);
        }
    }
}
