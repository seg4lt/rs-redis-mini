use anyhow::Context;

use crate::{LINE_ENDING, NEW_LINE};

#[derive(Debug, PartialEq)]
pub enum DataType {
    SimpleString(String),
    BulkString(String),
    Array(Vec<DataType>),
}

impl DataType {
    pub fn parse<R: std::io::BufRead>(reader: &mut R) -> anyhow::Result<DataType> {
        let mut buf = [0; 1];
        reader
            .read_exact(&mut buf)
            .context("Unable to determine DataType")?;
        match &buf[0] {
            b'*' => DataType::parse_array(reader).context("Unable to parse array"),
            b'$' => DataType::parse_bulk_string(reader).context("Unable to parse bulk string"),
            _ => todo!("not implemented"),
        }
    }
    fn parse_array<R: std::io::BufRead>(reader: &mut R) -> anyhow::Result<DataType> {
        let length = Self::read_count(reader).context("Unable to determine length of an array")?;
        let items = (0..length)
            .map(|i| {
                DataType::parse(reader)
                    .with_context(|| format!("Unable to parse {} indexed item", i))
                    .unwrap()
            })
            .collect::<Vec<DataType>>();
        Ok(DataType::Array(items))
    }
    fn parse_bulk_string<R: std::io::BufRead>(reader: &mut R) -> anyhow::Result<DataType> {
        let length =
            DataType::read_count(reader).context("Unable to read length of bulk string")?;
        let mut buf = vec![0; length + LINE_ENDING.len()];
        reader
            .read_exact(&mut buf)
            .context("Unable to read bulk string")?;
        let content = String::from_utf8(buf).context("Unable to convert buffer to string")?;
        if !content.ends_with(LINE_ENDING) {
            return Err(anyhow::anyhow!("Length doesn't match the size of content"));
        }
        Ok(DataType::BulkString(
            content[..content.len() - LINE_ENDING.len()].to_string(),
        ))
    }

    fn read_count<R: std::io::BufRead>(reader: &mut R) -> anyhow::Result<usize> {
        let mut buf = vec![];
        let read_count = reader
            .read_until(NEW_LINE, &mut buf) // TODO: What is \n was never sent?
            .context("Unable to read count")?;
        if read_count == 0 {
            return Err(anyhow::anyhow!("Zero bytes read - unable to read count"));
        }
        std::str::from_utf8(&buf[..buf.len() - LINE_ENDING.len()])
            .context("Unable to convert count buffer to string")?
            .parse::<usize>()
            .context("Unable to parse length from read count string")
    }
}

#[cfg(test)]
mod tests {
    use super::DataType;

    #[test]
    fn test_determine_type() {
        use std::io::Cursor;
        let mut cursor: Cursor<&str> = Cursor::new("*2\r\n$4\r\necho\r\n$3\r\nhey\r\n");
        let data_type = DataType::parse(&mut cursor).expect("Unable to determine type");
        assert_eq!(
            data_type,
            DataType::Array(vec![
                DataType::BulkString("echo".into()),
                DataType::BulkString("hey".into())
            ])
        );
    }
}
