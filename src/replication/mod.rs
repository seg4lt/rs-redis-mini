use std::borrow::BorrowMut;
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use tokio::io::BufReader;
use tokio::sync::Mutex;
use tokio::{net::TcpStream, sync::oneshot};

use crate::resp_type::RESPType;
use tokio::{io::AsyncWriteExt, sync::mpsc};

static EMITTER: OnceLock<ReplicationEventEmitter> = OnceLock::new();

#[derive(Debug)]
pub enum ReplicationEvent {
    SaveStream {
        host: String,
        port: u16,
        stream: TcpStream,
    },
    Set {
        key: String,
        value: String,
        flags: HashMap<String, String>,
    },
    GetNumOfReplicas {
        resp: oneshot::Sender<usize>,
    },
    GetAck {
        ack_wanted: usize,
        resp: mpsc::Sender<usize>,
    },
}
type ReplicationEventEmitter = mpsc::Sender<ReplicationEvent>;

impl ReplicationEvent {
    pub async fn emit(self) -> anyhow::Result<()> {
        let emitter = EMITTER.get_or_init(|| ReplicationEvent::setup());
        emitter.send(self).await?;
        Ok(())
    }
    pub fn setup() -> ReplicationEventEmitter {
        use ReplicationEvent::*;
        let (tx, mut rx) = mpsc::channel::<ReplicationEvent>(5);
        EMITTER.get_or_init(|| tx.clone());
        tokio::spawn(async move {
            let mut streams_map: HashMap<String, Arc<Mutex<TcpStream>>> = HashMap::new();
            while let Some(cmd) = rx.recv().await {
                match cmd {
                    SaveStream { host, port, stream } => {
                        let key = format!("{host}:{port}");
                        streams_map.insert(key, Arc::new(Mutex::new(stream)));
                    }
                    Set {
                        key,
                        value,
                        flags: _flags,
                    } => {
                        let msg = RESPType::Array(vec![
                            RESPType::BulkString("SET".to_string()),
                            RESPType::BulkString(key.clone()),
                            RESPType::BulkString(value.clone()),
                        ]);
                        for (_, v) in streams_map.borrow_mut() {
                            let mut stream = v.lock().await;
                            let _ = stream.write_all(&msg.as_bytes()).await;
                            stream.flush().await.unwrap();
                        }
                    }
                    GetNumOfReplicas { resp: recv_chan } => {
                        let _ = recv_chan.send(streams_map.len());
                    }
                    GetAck {
                        ack_wanted: min_ack,
                        resp,
                    } => get_ack(&streams_map, min_ack, resp).await.unwrap(),
                }
            }
        });
        tx
    }
}

async fn get_ack(
    streams_map: &HashMap<String, Arc<Mutex<TcpStream>>>,
    min_ack: usize,
    resp: mpsc::Sender<usize>,
) -> anyhow::Result<()> {
    let mut acks_received = 0;

    for (_, stream) in streams_map {
        let mut cl_stream = Arc::clone(stream);
        let resp = resp.clone();
        tokio::spawn(async move {
            let req = RESPType::Array(vec![
                RESPType::BulkString("REPLCONF".to_string()),
                RESPType::BulkString("GETACK".to_string()),
                RESPType::BulkString("*".to_string()),
            ]);
            let mut stream = cl_stream.borrow_mut().lock().await;
            let (reader, mut writer) = stream.split();
            let _ = writer.write_all(&req.as_bytes()).await;
            writer.flush().await.unwrap();
            let mut reader = BufReader::new(reader);

            // Probably need a better way to cancel this task after get ack was received
            match tokio::time::timeout(Duration::from_millis(100), RESPType::parse(&mut reader))
                .await
            {
                // match RESPType::parse(&mut reader).await {
                Ok(..) => {
                    tracing::debug!("Got an ack");
                    acks_received += 1;
                    let _ = resp.send(1).await;
                }
                Err(..) => {
                    tracing::debug!("No ack received")
                }
            }
            tracing::debug!(?acks_received, ?min_ack, "Checking if we have enough acks");
        });
    }
    Ok(())
}
