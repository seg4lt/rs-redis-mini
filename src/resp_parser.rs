use anyhow::{anyhow, bail, Context};

use crate::{fdbg, LINE_ENDING, NEW_LINE};

#[derive(Debug, PartialEq)]
pub enum DataType {
    SimpleString(String),
    BulkString(String),
    NullBulkString,
    NotBulkString(Vec<u8>),
    Array(Vec<DataType>),
    Noop,
}

impl DataType {
    pub fn as_bytes(&self) -> Vec<u8> {
        match &self {
            DataType::SimpleString(s) => format!("+{}{LINE_ENDING}", s).into_bytes(),
            DataType::BulkString(s) => {
                format!("${}{LINE_ENDING}{}{LINE_ENDING}", s.len(), s).into_bytes()
            }
            DataType::NotBulkString(s) => {
                let mut result = format!("${}{LINE_ENDING}", s.len()).into_bytes();
                result.extend(s);
                return result;
            }
            DataType::NullBulkString => format!("${}{LINE_ENDING}", "-1").into_bytes(),
            DataType::Array(items) => {
                let mut result = Vec::from(format!("*{}{LINE_ENDING}", items.len()).into_bytes());
                for item in items {
                    result.extend(item.as_bytes())
                }
                result
            }
            DataType::Noop => vec![],
        }
    }
    pub fn parse<R: std::io::BufRead>(reader: &mut R) -> anyhow::Result<DataType> {
        let mut buf = [0; 1];
        let read_count = match reader.read(&mut buf) {
            Ok(read_count) => read_count,
            Err(_) => {
                println!("⭕️ >>> Unable to read anything. Setting read count to 0");
                0
            }
        };
        if read_count == 0 {
            // Unable to read anything, so noop is sent
            return Ok(DataType::Noop);
        }
        // println!("⭕️ >>> Read: {:?}", buf[0] as char);
        match &buf[0] {
            b'*' => DataType::parse_array(reader).context(fdbg!("Unable to parse array")),
            b'$' => {
                DataType::parse_bulk_string(reader).context(fdbg!("Unable to parse bulk string"))
            }
            b'+' => DataType::parse_simple_string(reader)
                .context(fdbg!("Unable to parse simple string")),
            _ => Err(anyhow!("Unknown DataType"))?,
        }
    }
    fn parse_simple_string<R: std::io::BufRead>(reader: &mut R) -> anyhow::Result<DataType> {
        let mut buf = vec![];
        let read_count = reader
            .read_until(NEW_LINE, &mut buf)
            .context(fdbg!("Unable to read simple string"))?;
        if read_count == 0 {
            return Err(anyhow!("Zero bytes read - unable to read simple string"));
        }
        Ok(DataType::SimpleString(
            std::str::from_utf8(&buf[..buf.len() - LINE_ENDING.len()])
                .context(fdbg!("Unable to convert simple string buffer to string"))?
                .to_string(),
        ))
    }
    fn parse_array<R: std::io::BufRead>(reader: &mut R) -> anyhow::Result<DataType> {
        let length =
            Self::read_count(reader).context(fdbg!("Unable to determine length of an array"))?;
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
            DataType::read_count(reader).context(fdbg!("Unable to read length of bulk string"))?;
        let mut content_buf = vec![0; length + LINE_ENDING.len()];
        let read_count = reader
            .read(&mut content_buf)
            .context(fdbg!("Unable to read content of bulk string"))?;
        if read_count == length {
            println!("⭕️ >>> LINE_ENDING not found, setting the type to NotBulkString");
            return Ok(DataType::NotBulkString(content_buf[..(length)].to_vec()));
        }
        match String::from_utf8(content_buf.clone())
            .context(fdbg!("Unable to convert buffer to utf8"))
        {
            Ok(content) => Ok(DataType::BulkString(content[..length].to_string())),
            Err(err) => {
                bail!("Unable to read bulk string, {:?}", err)
            }
        }
    }

    fn read_count<R: std::io::BufRead>(reader: &mut R) -> anyhow::Result<usize> {
        let mut buf = vec![];
        let read_count = reader
            .read_until(NEW_LINE, &mut buf) // TODO: What is \n was never sent?
            .context(fdbg!("Unable to read count"))?;
        if read_count == 0 {
            return Err(anyhow!("Zero bytes read - unable to read count"));
        }
        std::str::from_utf8(&buf[..buf.len() - LINE_ENDING.len()])
            .context(fdbg!("Unable to convert count buffer to string"))?
            .parse::<usize>()
            .context(fdbg!("Unable to parse length from read count string"))
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
                    "*5\r\n$3\r\nset\r\n$4\r\nkey2\r\n$6\r\nvalus2\r\n$2\r\npx\r\n$4\r\n1000\r\n",
                expected: DataType::Array(vec![
                    DataType::BulkString("set".into()),
                    DataType::BulkString("key2".into()),
                    DataType::BulkString("valus2".into()),
                    DataType::BulkString("px".into()),
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
