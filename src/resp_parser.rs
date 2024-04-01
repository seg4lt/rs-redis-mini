use std::io::BufRead;

use anyhow::{bail, Context};
use tracing::{debug, info, warn};

use crate::{fdbg, LINE_ENDING, NEW_LINE};

#[derive(Debug, PartialEq)]
pub enum DataType {
    SimpleString(String),
    BulkString(String),
    NullBulkString,
    Array(Vec<DataType>),
    RDSFile(Vec<u8>),
    Integer(u64),
    EmptyString,
    NewLine(char),
}

impl DataType {
    pub fn as_bytes(&self) -> Vec<u8> {
        match &self {
            DataType::SimpleString(s) => format!("+{}{LINE_ENDING}", s).into_bytes(),
            DataType::BulkString(s) => {
                format!("${}{LINE_ENDING}{}{LINE_ENDING}", s.len(), s).into_bytes()
            }
            DataType::RDSFile(s) => {
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
            DataType::Integer(i) => format!(":{}{LINE_ENDING}", i).into_bytes(),
            DataType::EmptyString | DataType::NewLine(_) => vec![],
        }
    }
    pub fn parse<R: BufRead>(reader: &mut R) -> anyhow::Result<DataType> {
        let data_type = Self::parse_inner(reader)
            .context(fdbg!("Unable to read DataType to process command"))?;
        match data_type {
            DataType::RDSFile(_) => debug!("🔥 Received RDS File"),
            DataType::NewLine(ch) => debug!("🔥 Received NewLine {:?}", ch),
            _ => debug!("🔥 Received {:?}", String::from_utf8(data_type.as_bytes())?),
        }
        Ok(data_type)
    }
    fn parse_inner<R: BufRead>(reader: &mut R) -> anyhow::Result<DataType> {
        let mut buf = [0; 1];
        let read_count = match reader.read(&mut buf) {
            Ok(read_count) => read_count,
            Err(_) => {
                warn!("Unable to read anything. Setting read count to 0");
                0
            }
        };
        if read_count == 0 {
            // Unable to read anything, so noop is sent
            return Ok(DataType::EmptyString);
        }
        // debug!("DataType identifier {:?}", buf[0].to_owned() as char);
        match &buf[0] {
            b'*' => DataType::parse_array(reader).context(fdbg!("Unable to parse array")),
            b'$' => {
                DataType::parse_bulk_string(reader).context(fdbg!("Unable to parse bulk string"))
            }
            b'+' => DataType::parse_simple_string(reader)
                .context(fdbg!("Unable to parse simple string")),
            b'\r' | b'\n' => Ok(DataType::NewLine(buf[0].to_owned() as char)),
            foo => unimplemented!(
                "Converting this data type is not implemented yet !!! - {:?}",
                foo.to_owned() as char
            ),
        }
    }
    fn parse_simple_string<R: BufRead>(reader: &mut R) -> anyhow::Result<DataType> {
        let mut buf = vec![];
        let read_count = reader
            .read_until(NEW_LINE, &mut buf)
            .context(fdbg!("Unable to read simple string"))?;
        if read_count == 0 {
            bail!("Zero bytes read - unable to read simple string");
        }
        let msg = std::str::from_utf8(&buf[..buf.len() - LINE_ENDING.len()])
            .context(fdbg!("Unable to convert simple string buffer to string"))?
            .to_string();
        debug!("Simple string content: {:?}", msg);
        Ok(DataType::SimpleString(msg))
    }
    fn parse_array<R: BufRead>(reader: &mut R) -> anyhow::Result<DataType> {
        let length =
            Self::read_count(reader).context(fdbg!("Unable to determine length of an array"))?;
        debug!("Array length: {}", length);
        let items = (0..length)
            .map(|i| {
                DataType::parse(reader)
                    .with_context(|| format!("Unable to parse {} indexed item", i))
                    .unwrap()
            })
            .collect::<Vec<DataType>>();
        Ok(DataType::Array(items))
    }
    fn parse_bulk_string<R: BufRead>(reader: &mut R) -> anyhow::Result<DataType> {
        let length =
            DataType::read_count(reader).context(fdbg!("Unable to read length of bulk string"))?;
        let mut content_buf = vec![0; length + LINE_ENDING.len()];
        reader
            .read_exact(&mut content_buf)
            .context(fdbg!("Unable to read content of bulk string"))?;

        let content = String::from_utf8(content_buf[..length].to_vec())
            .context(fdbg!("Unable to convert bulk string to string"))?;
        // debug!("Bulk string content: {:?}", content);
        Ok(DataType::BulkString(content))
    }
    pub fn parse_rds_string<R: BufRead>(reader: &mut R) -> anyhow::Result<DataType> {
        let mut buf = [0; 1];
        reader
            .read_exact(&mut buf)
            .context(fdbg!("Reading the datatype identifier for rds string"))?;
        if buf[0] != b'$' {
            bail!("Invalid RDS string identifier");
        }
        let length = DataType::read_count(reader)
            .context(fdbg!("Unable to read length of RDSRDSRDS string"))?;
        let mut content_buf = vec![0; length];

        reader
            .read_exact(&mut content_buf)
            .context(fdbg!("Unable to read content of bulk string"))?;

        info!(
            "Looks like RDS file, first 5 bytes are {:?}",
            std::str::from_utf8(&content_buf[..5])?
        );
        Ok(DataType::RDSFile(content_buf[..(length)].to_vec()))
    }
    fn read_count<R: BufRead>(reader: &mut R) -> anyhow::Result<usize> {
        let mut buf = vec![];
        let read_count = reader
            .read_until(NEW_LINE, &mut buf) // TODO: What is \n was never sent?
            .context(fdbg!("Unable to read count"))?;
        if read_count == 0 {
            bail!("Zero bytes read - unable to read count");
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
