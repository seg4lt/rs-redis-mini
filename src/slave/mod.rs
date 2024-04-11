use tokio::{
    io::{AsyncWriteExt, BufReader},
    net::{
        tcp::{ReadHalf, WriteHalf},
        TcpStream,
    },
};
use tracing::debug;

use crate::{
    app_config::AppConfig,
    cmd_parser::{client_cmd::ClientCmd, slave_cmd::SlaveCmd},
    resp_type::RESPType,
};

pub struct Slave {}
impl Slave {
    pub async fn setup() -> anyhow::Result<()> {
        if AppConfig::is_master() {
            return Ok(());
        }
        debug!("Starting connection with master");
        let Some((host, port)) = AppConfig::get_replicaof() else {
            panic!("Replica should have --replicaof args");
        };
        let mut stream = TcpStream::connect(format!("{host}:{port}")).await?;
        tokio::spawn(async move {
            let (reader, mut writer) = stream.split();
            let mut reader = BufReader::new(reader);
            handshake(&mut writer, &mut reader).await;
            receive_rdb_file(&mut reader).await;
            let mut bytes_received = 0;
            loop {
                let resp_type = RESPType::parse(&mut reader).await.unwrap();
                let client_cmd = ClientCmd::from_resp_type(&resp_type).unwrap();
                let slave_cmd = SlaveCmd::from_client_cmd(&client_cmd).unwrap();
                slave_cmd
                    .process_slave_cmd(&mut writer, bytes_received)
                    .await
                    .unwrap();
                writer.flush().await.unwrap();
                bytes_received += resp_type.as_bytes().len();
                debug!("Bytes received: {bytes_received}");
            }
        });
        Ok(())
    }
}

async fn receive_rdb_file(reader: &mut BufReader<ReadHalf<'_>>) {
    RESPType::parse_rdb_file(reader)
        .await
        .expect("Should be able to parse RDB file");
}

async fn handshake<'a>(writer: &mut WriteHalf<'_>, reader: &mut BufReader<ReadHalf<'_>>) {
    // PING
    let ping = RESPType::Array(vec![RESPType::BulkString("PING".to_string())]);
    writer
        .write_all(&ping.as_bytes())
        .await
        .expect("Should be able to write PING");
    writer.flush().await.expect("Should be able to flush PING");
    let _response = RESPType::parse(reader)
        .await
        .expect("Should be able to parse PONG");
    // REPL CONF
    let port = AppConfig::get_port();
    let repl_conf_listening_port = RESPType::Array(vec![
        RESPType::BulkString("REPLCONF".to_string()),
        RESPType::BulkString("listening-port".to_string()),
        RESPType::BulkString(format!("{port}")),
    ]);
    writer
        .write_all(&repl_conf_listening_port.as_bytes())
        .await
        .expect("Should be able to write replconf listening-port");
    writer
        .flush()
        .await
        .expect("Should be able to flush replconf listening-port");
    let _response = RESPType::parse(reader)
        .await
        .expect("Should be able to parse OK");

    // REPL capa psync2
    let repl_conf_capa_psync2 = RESPType::Array(vec![
        RESPType::BulkString("REPLCONF".to_string()),
        RESPType::BulkString("capa".to_string()),
        RESPType::BulkString("psync2".to_string()),
    ]);
    writer
        .write_all(&repl_conf_capa_psync2.as_bytes())
        .await
        .expect("Should be able to write replconf capa psync2");
    writer
        .flush()
        .await
        .expect("Should be able to flush replconf capa psync2");
    let _response = RESPType::parse(reader)
        .await
        .expect("Should be able to parse OK");

    // PSYNC
    let repl_conf_capa_psync2 = RESPType::Array(vec![
        RESPType::BulkString("PSYNC".to_string()),
        RESPType::BulkString("?".to_string()),
        RESPType::BulkString("-1".to_string()),
    ]);
    writer
        .write_all(&repl_conf_capa_psync2.as_bytes())
        .await
        .expect("Should be able to write psync ? -1");
    writer
        .flush()
        .await
        .expect("Should be able to flush psync ? -1");
    let _response = RESPType::parse(reader)
        .await
        .expect("Should be able to parse FULLRESYNC");
}
