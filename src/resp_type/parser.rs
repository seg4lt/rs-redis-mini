use anyhow::{bail, Context};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, BufReader},
    net::tcp::ReadHalf,
};
use tracing::debug;

use async_recursion::async_recursion;

use crate::{fdbg, LINE_ENDING, NEW_LINE};

use super::RESPType;

type Reader<'a> = BufReader<ReadHalf<'a>>;

impl RESPType {
    // TODO: Need to study more on Box::pin
    #[async_recursion]
    pub async fn parse<'a>(reader: &'a mut BufReader<ReadHalf<'_>>) -> anyhow::Result<RESPType> {
        // debug!("Trying to read from client");
        let mut buf = [0; 1];
        let count = reader
            .read(&mut buf)
            .await
            .context(fdbg!("Unable to read string from client"))
            .unwrap_or(0);
        if count == 0 {
            debug!("GOT EOF");
            return Ok(RESPType::EOF);
        }
        // debug!("DataType identifying ASCII:({})", buf[0]);
        let request_resp_type = match buf[0] {
            b'*' => {
                let count = RESPType::read_count(reader).await?;
                let mut items: Vec<RESPType> = Vec::with_capacity(count);
                // debug!("Count of items in request - {}", count);
                for _ in 0..count {
                    let item = RESPType::parse(reader).await?;
                    items.push(item);
                }
                RESPType::Array(items)
            }
            b'$' => RESPType::read_bulk_string(reader).await?,
            b'+' => RESPType::read_simple_string(reader).await?,
            b'\n' | b'\r' => RESPType::CustomNewLine,
            b'`' => RESPType::read_custom_command(reader).await?,
            _ => bail!("Unable to determine data type: {}", buf[0] as char),
        };
        // debug!("Request RESPType - {:?}", request_resp_type);
        Ok(request_resp_type)
    }

    pub(crate) async fn read_custom_command<'a>(
        reader: &'a mut Reader<'_>,
    ) -> anyhow::Result<RESPType> {
        let mut buf = String::new();
        reader.read_line(&mut buf).await?;
        let items = buf
            .split(" ")
            .map(|x| RESPType::BulkString(x.to_string()))
            .collect::<Vec<RESPType>>();
        Ok(RESPType::Array(items))
    }

    pub(crate) async fn read_simple_string<'a>(
        reader: &'a mut Reader<'_>,
    ) -> anyhow::Result<RESPType> {
        let mut buf = vec![];
        reader.read_until(NEW_LINE, &mut buf).await?;
        let content = std::str::from_utf8(&buf[..buf.len() - LINE_ENDING.len()])
            .context(fdbg!("Unable to convert bytes to string"))?;
        Ok(RESPType::SimpleString(content.to_string()))
    }

    pub async fn read_bulk_string<'a>(
        reader: &'a mut BufReader<ReadHalf<'_>>,
    ) -> anyhow::Result<RESPType> {
        let length = RESPType::read_count(reader).await?;
        let mut buf = vec![0; length + LINE_ENDING.len()];
        reader
            .read_exact(&mut buf)
            .await
            .context(fdbg!("Unable to read string from reader"))?;
        let string = std::str::from_utf8(&buf[..length])
            .context(fdbg!("Unable to convert bytes to string"))?
            .to_string();
        Ok(RESPType::BulkString(string))
    }

    pub async fn read_count<'a>(reader: &'a mut BufReader<ReadHalf<'_>>) -> anyhow::Result<usize> {
        let mut buf = vec![];
        let read_count = reader
            .read_until(NEW_LINE, &mut buf)
            .await
            .context(fdbg!("Unable to read length of data"))?;
        // debug!("Read count {read_count}");
        let number_part = &buf[..(read_count - LINE_ENDING.len())];
        let length = std::str::from_utf8(number_part)
            .context(fdbg!("Unable to convert length to string"))?
            .parse::<usize>()
            .context(fdbg!("Unable to parse length to usize"))?;
        Ok(length)
    }

    pub async fn parse_rdb_file<'a>(reader: &'a mut BufReader<ReadHalf<'_>>) -> anyhow::Result<()> {
        let mut buf = [0; 1];
        reader.read_exact(&mut buf).await?;
        debug!("RESP first byte {}", buf[0]);
        let length = RESPType::read_count(reader).await?;
        let mut content = vec![0; length];
        reader.read_exact(&mut content).await?;
        debug!("Read {} length data", content.len());
        Ok(())
    }
}
