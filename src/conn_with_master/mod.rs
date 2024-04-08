use tokio::{
    io::{AsyncWriteExt, BufReader},
    net::TcpStream,
};
use tracing::{debug, span, Level};

use crate::{
    app_config::AppConfig,
    resp_type::{parser::parse_request, RESPType},
};

pub(crate) async fn prepare_conn_with_master() -> anyhow::Result<()> {
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
        // PING
        let ping = RESPType::Array(vec![RESPType::BulkString("PING".to_string())]);
        writer
            .write_all(&ping.as_bytes())
            .await
            .expect("Should be able to write PING");
        writer.flush().await.expect("Should be able to flush PING");
        let _response = parse_request(&mut reader)
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
        // let _response = parse_request(&mut reader)
        //     .await
        //     .expect("Should be able to parse OK");

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
        // let _response = parse_request(&mut reader)
        //     .await
        //     .expect("Should be able to parse OK");
    });
    Ok(())
}
