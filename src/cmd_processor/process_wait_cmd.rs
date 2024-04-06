use std::{io::Write, net::TcpStream, sync::Arc, time::Instant};

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
            debug!("Sending get ack to index({})", i);
            let mut stream = replicas
                .unwrap()
                .get(i)
                .context(fdbg!("[{}] Unable to get replica from cache", i))?;
            stream
                .write_all(&get_ack_command.as_bytes())
                .context(fdbg!("[{}] Unable to write to stream for get ack", i))?;
            let (cmd, flag, offset) = run_get_ack(stream)?;
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

// pub fn process_wait_cmd(
//     nacks_wanted: usize,
//     timeout_ms: u64,
//     map: &Arc<Store>,
//     replicas: Option<&Arc<Replicas>>,
// ) -> anyhow::Result<DataType> {
//     let span = span!(Level::DEBUG, "process_wait_cmd");
//     let _guard = span.enter();

//     let replicas = match replicas {
//         None => {
//             debug!("No replicas to wait for. Returning 0 as acks received.");
//             return Ok(DataType::Integer(0));
//         }
//         Some(r) => r.to_owned(),
//     };
//     trace!(
//         "Number of acks wanted: {}, timeout_ms: {}",
//         nacks_wanted,
//         timeout_ms
//     );
//     trace!("Number of replicas I have right now - {:?}", replicas.len());
//     map.set(
//         KEY_IS_WAIT_RUNNING.into(),
//         "true".into(),
//         Some(Duration::from_millis(timeout_ms)),
//     );

//     let ack_received = Arc::new(Mutex::new(0 as usize));
//     let mut is_waiting = map
//         .get(KEY_IS_WAIT_RUNNING.into())
//         .unwrap_or("false".into());

//     'wait_loop: loop {
//         debug!("Waiting for replicas to ack");
//         if is_waiting == "false" {
//             debug!("Wait timeout. Returning acks received till now.");
//             break 'wait_loop;
//         }
//         if *ack_received.lock().unwrap() >= nacks_wanted {
//             debug!("Received required number acks. Returning");
//             break 'wait_loop;
//         }
//         let num_replicas = replicas.len();
//         debug!("Number of replica we have right now -- {num_replicas:?}");
//         for i in 0..num_replicas {
//             let (c_ack_received, c_replicas) = (ack_received.clone(), replicas.clone());
//             std::thread::spawn(move || {
//                 let span = span!(Level::DEBUG, "process_wait_cmd_thread");
//                 let _guard = span.enter();
//                 debug!("This is the value of i -- {i:?}");
//                 let stream = c_replicas
//                     .clone()
//                     .get(i)
//                     .context(fdbg!("[{i}] Unable to get replica from cache"))
//                     .expect("[{i}] Replica not found");
//                 debug!("[{i}] Asking for GETACK");
//                 let Ok((cmd, flag, offset)) = run_get_ack(stream) else {
//                     return;
//                 };
//                 debug!(
//                     "[{i}] Received ack from replica: {} {} {}",
//                     cmd, flag, offset
//                 );
//                 let mut c_ack_received = c_ack_received.lock().unwrap();
//                 *c_ack_received += 1;
//             });
//         }

//         std::thread::sleep(Duration::from_millis(100));
//         is_waiting = map
//             .get(KEY_IS_WAIT_RUNNING.into())
//             .unwrap_or("false".into());
//     }
//     return Ok(DataType::Integer(*ack_received.lock().unwrap() as u64));
// }

fn run_get_ack(mut stream: TcpStream) -> anyhow::Result<(String, String, usize)> {
    let get_ack_msg = DataType::Array(vec![
        DataType::BulkString("REPLCONF".into()),
        DataType::BulkString("GETACK".into()),
        DataType::BulkString("*".into()),
    ]);
    stream
        .write_all(&get_ack_msg.as_bytes())
        .context(fdbg!("Unable to write to stream for get ack"))?;

    let mut reader = std::io::BufReader::new(&stream);
    DataType::parse_replconf_ack_offset(&mut reader)
}
