use std::{collections::HashMap, time::SystemTime};

use anyhow::{bail, Context};
use tokio::{
    fs::File,
    io::{AsyncBufRead, AsyncReadExt, BufReader},
};
use tracing::debug;

use crate::{app_config::AppConfig, binary, database::Database, fdbg};

pub(crate) async fn parse_rdb_file() -> anyhow::Result<()> {
    let dir = AppConfig::get_rds_dir();
    let rdb_file = AppConfig::get_rds_file_name();
    let Ok(file) = File::open(format!("{}/{}", dir, rdb_file)).await else {
        return Ok(());
    };
    let mut reader = BufReader::new(file);

    let file_header = read_string_encoded(&mut reader, 5).await?;
    let version = read_string_encoded(&mut reader, 4).await?;
    tracing::debug!("{} {}", file_header, version);
    loop {
        let op_code = read_bytes(&mut reader, 1)
            .await
            .context(fdbg!("Unable to read OpCode"))?;
        match op_code[0] {
            0xFF => {
                debug!("EOF file found");
                break;
            }
            0xFA => {
                // Auxiliary fields
                let key = read_string_encoded_key(&mut reader).await?;
                let value = read_length_encoded_key_as_string(&mut reader).await?;
                debug!("AUX {key} = {value}");
            }
            0xFE => {
                let value = read_length(&mut reader, 1).await?;
                debug!("Database selector = {value}")
            }
            // "expiry time in seconds", followed by 4 byte unsigned int
            0xFD => {
                let time = read_length(&mut reader, 4).await?;
                debug!("Reader time seconds = {time}");
                // Not implemented for now
                // let (key, value) = read_key_value(&mut reader, op_code[0]).await?;
                // rdb_map.insert(key, value);
            }
            // "expiry time in ms", followed by 8 byte unsigned long
            0xFC => {
                let time = read_bytes(&mut reader, 8).await?;
                let time: [u8; 8] = time[..8].try_into().unwrap();
                let exp_time = u64::from_le_bytes(time) as u128;
                let now = SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap();
                let now_millis = now.as_millis();
                let value_type = read_bytes(&mut reader, 1)
                    .await
                    .context(fdbg!("Unable to read value type"))?;
                let (key, value) = read_key_value(&mut reader, value_type[0]).await?;
                if exp_time > now_millis {
                    let diff = exp_time - now_millis;
                    let mut map: HashMap<String, String> = HashMap::new();
                    map.insert("px".to_string(), diff.to_string());
                    Database::set(&key, &value, &map).await?;
                }
            }
            0xFB => {
                debug!("Resize DB OP code found");
                let size_of_corresponding_hash_table = read_length(&mut reader, 1).await?;
                let size_of_corresponding_expire_hash_table = read_length(&mut reader, 1).await?;
                debug!("Size of corresponding hash table = {size_of_corresponding_hash_table}");
                debug!("Size of corresponding exprire hash table = {size_of_corresponding_expire_hash_table}");
            }
            // Not special character, Try to read the data
            _ => {
                let (key, value) = read_key_value(&mut reader, op_code[0]).await?;
                Database::set(&key, &value, &HashMap::new()).await?;
            }
        }
    }
    Ok(())
}

async fn read_key_value<R>(reader: &mut R, value_type: u8) -> anyhow::Result<(String, String)>
where
    R: AsyncBufRead + AsyncReadExt + Unpin,
{
    let value = match value_type {
        // 0 = String Encoding
        0 => {
            let key = read_string_encoded_key(reader).await?;
            let value = read_string_encoded_key(reader).await?;
            (key, value)
        }
        // Other encoding not imported yet
        _ => bail!("Unknown OpCode"),
    };
    Ok(value)
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
