use anyhow::Context;

use crate::{LINE_ENDING, NEW_LINE};

#[derive(Debug, PartialEq)]
pub enum DataType {
    SimpleString(String),
    BulkString(String),
    NullBulkString,
    Array(Vec<DataType>),
    Noop,
}

impl DataType {
    pub fn to_string(&self) -> String {
        match self {
            DataType::SimpleString(s) => format!("+{}{LINE_ENDING}", s),
            DataType::BulkString(s) => format!("${}{LINE_ENDING}{}{LINE_ENDING}", s.len(), s),
            DataType::NullBulkString => format!("${}{LINE_ENDING}", "-1"),
            DataType::Array(items) => {
                let mut result = format!("*{}{LINE_ENDING}", items.len());
                for item in items {
                    result.push_str(&item.to_string());
                }
                result
            }
            DataType::Noop => "".to_string(),
        }
    }
    pub fn parse<R: std::io::BufRead>(reader: &mut R) -> anyhow::Result<DataType> {
        let mut buf = [0; 1];
        let read_count = reader
            .read(&mut buf)
            .context("Unable to determine DataType")?;
        if read_count == 0 {
            // Unable to read anything, so noop is sent
            return Ok(DataType::Noop);
        }
        match &buf[0] {
            b'*' => DataType::parse_array(reader).context("Unable to parse array"),
            b'$' => DataType::parse_bulk_string(reader).context("Unable to parse bulk string"),
            _ => Err(anyhow::anyhow!("Unknown DataType"))?,
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
        struct Test {
            input: &'static str,
            expected: DataType,
        }
        let tests = vec![
            Test {
                input: "*2\r\n$4\r\necho\r\n$3\r\nhey\r\n",
                expected: DataType::Array(vec![
                    DataType::BulkString("echo".into()),
                    DataType::BulkString("hey".into()),
                ]),
            },
            Test {
                input: "*3\r\n$3\r\nset\r\n$3\r\nkey\r\n$5\r\nvalue\r\n",
                expected: DataType::Array(vec![
                    DataType::BulkString("set".into()),
                    DataType::BulkString("key".into()),
                    DataType::BulkString("value".into()),
                ]),
            },
            Test {
                input: "*2\r\n$3\r\nget\r\n$3\r\nkey\r\n",
                expected: DataType::Array(vec![
                    DataType::BulkString("get".into()),
                    DataType::BulkString("key".into()),
                ]),
            },
            Test {
                input:
                    "*5\r\n$3\r\nset\r\n$4\r\nkey2\r\n$6\r\nvalus2\r\n$2\r\nex\r\n$4\r\n1000\r\n",
                expected: DataType::Array(vec![
                    DataType::BulkString("set".into()),
                    DataType::BulkString("key2".into()),
                    DataType::BulkString("valus2".into()),
                    DataType::BulkString("ex".into()),
                    DataType::BulkString("1000".into()),
                ]),
            },
            Test {
                input: "*2\r\n$4\r\nINFO\r\n$11\r\nreplication\r\n",
                expected: DataType::Array(vec![
                    DataType::BulkString("INFO".into()),
                    DataType::BulkString("replication".into()),
                ]),
            },
        ];

        for test in tests {
            let mut cursor: Cursor<&str> = Cursor::new(test.input);
            let data_type = DataType::parse(&mut cursor).expect("Unable to determine type");
            assert_eq!(data_type, test.expected);
        }
    }
}
