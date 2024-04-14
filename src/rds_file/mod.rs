use std::collections::HashMap;

use anyhow::Context;
use tokio::{
    fs::File,
    io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, BufReader},
};
use tracing::debug;

use crate::{
    app_config::AppConfig,
    binary,
    database::{Database, DatabaseEvent},
    fdbg,
};

pub(crate) async fn parse_rdb_file() -> anyhow::Result<HashMap<String, String>> {
    let mut rdb_map: HashMap<String, String> = HashMap::new();
    let dir = AppConfig::get_rds_dir();
    let rdb_file = AppConfig::get_rds_file_name();
    let Ok(file) = File::open(format!("{}/{}", dir, rdb_file)).await else {
        return Ok(rdb_map);
    };
    let mut rdb_reader = BufReader::new(file);

    let file_header = read_string_encoded(&mut rdb_reader, 5).await?;
    let version = read_string_encoded(&mut rdb_reader, 4).await?;
    tracing::debug!("{} {}", file_header, version);
    let mut i = 0;

    loop {
        let op_code = read_bytes(&mut rdb_reader, 1)
            .await
            .context(fdbg!("Unable to read OpCode"))?;
        // tracing::debug!("OpCode = {:x?}", &op_code[0]);
        match op_code[0] {
            0xFF => {
                debug!("EOF file found");
                break;
            }
            0xFA => {
                // Auxiliary fields
                let key = read_string_encoded_key(&mut rdb_reader).await?;
                let value = read_length_encoded_key_as_string(&mut rdb_reader).await?;
                debug!("AUX {key} = {value}");
                i += 1;
                if i == 6 {
                    break;
                }
            }
            0xFE => {
                let value = read_length(&mut rdb_reader, 1).await?;
                debug!("Database selector = {value}")
            }
            0xFB => {
                debug!("Resize DB OP code found");
                let size_of_corresponding_hash_table = read_length(&mut rdb_reader, 1).await?;
                let size_of_corresponding_expire_hash_table =
                    read_length(&mut rdb_reader, 1).await?;
                debug!("Size of corresponding hash table = {size_of_corresponding_hash_table}");
                debug!("Size of corresponding exprire hash table = {size_of_corresponding_expire_hash_table}");
            }
            // Not special character, Try to read the data
            _ => match op_code[0] {
                // 0 = String Encoding
                0 => {
                    let key = read_string_encoded_key(&mut rdb_reader).await?;
                    let value = read_string_encoded_key(&mut rdb_reader).await?;
                    debug!("Key = {key}, Value = {value}");
                    rdb_map.insert(key, value);
                }
                // Other encoding not imported yet
                _ => debug!("Unknown OpCode"),
            },
        }
    }

    for (k, v) in &rdb_map {
        Database::emit(DatabaseEvent::Set {
            key: k.to_string(),
            value: v.to_string(),
            flags: HashMap::new(),
        })
        .await?;
    }

    Ok(rdb_map)
}

async fn read_length_encoded_key_as_string<R>(reader: &mut R) -> anyhow::Result<String>
where
    R: AsyncBufRead + AsyncReadExt + Unpin,
{
    let value = read_length(reader, 1)
        .await
        .context(fdbg!("Unable to read value as string"))?;
    // debug!("Value = {value}, {value:x}, {value:0>8b}");
    let msb = binary!(@msb; value as u8, 2);
    let value = match msb {
        // The next 6 bits represent the length
        0b00 => {
            let lsb = binary!(@lsb; value as u8, 6);
            read_string_encoded(reader, lsb as usize).await?
        }
        // Read one additional byte. The combined 14 bits represent the length
        0b01 => unimplemented!("0b01 not implemented yet"),
        // Discard the remaining 6 bits. The next 4 bytes from the stream represent the length
        0b10 => unimplemented!("0b10 not implemented yet"),
        // The next object is encoded in a special format. The remaining 6 bits indicate the format. May be used to store numbers or Strings, see String Encoding
        0b11 => {
            let lsb = binary!(@lsb; value as u8, 6);
            let bytes_to_read_for_length = match lsb {
                // 0 indicates that an 8 bit integer follows
                0 => 1,
                // 1 indicates that a 16 bit integer follows
                1 => 2,
                // 2 indicates that a 32 bit integer follows
                2 => 4,
                _ => unimplemented!("Unknown value"),
            };
            let length = read_length(reader, bytes_to_read_for_length)
                .await
                .context(fdbg!("Unable to read length of value"))?;
            format!("{}", length)
        }
        _ => unimplemented!("Unknown value"),
    };

    Ok(format!("{value}"))
}

async fn read_string_encoded_key<R>(reader: &mut R) -> anyhow::Result<String>
where
    R: AsyncBufRead + AsyncReadExt + Unpin,
{
    let key_length = read_length(reader, 1)
        .await
        .context(fdbg!("Unable to read key_length of key"))?;
    // debug!("Key length = {key_length}, {key_length:x}");
    let key = read_string_encoded(reader, key_length)
        .await
        .context(fdbg!("Unable to read key as string"))?;
    Ok(key)
}

async fn read_length<R>(reader: &mut R, n: usize) -> anyhow::Result<usize>
where
    R: AsyncBufRead + AsyncReadExt + Unpin,
{
    let buf = read_bytes(reader, n).await?;
    let length = buf[0];
    return Ok(length as usize);
}

async fn read_string_encoded<R>(reader: &mut R, n: usize) -> anyhow::Result<String>
where
    R: AsyncBufRead + AsyncReadExt + Unpin,
{
    let buf = read_bytes(reader, n).await?;
    let content = String::from_utf8(buf)?;
    return Ok(content);
}

async fn read_bytes<R>(reader: &mut R, n: usize) -> anyhow::Result<Vec<u8>>
where
    R: AsyncBufRead + AsyncReadExt + Unpin,
{
    let mut buf = vec![0; n];
    reader.read_exact(&mut buf).await?;
    return Ok(buf);
}
