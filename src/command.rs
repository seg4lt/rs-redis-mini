use anyhow::{Context, Ok};

use crate::resp_parser::DataType;

#[derive(Debug, PartialEq)]
pub enum Command {
    Ping(Option<String>),
    Echo(String),
    Set(String, String),
    Get(String),
    Noop,
}

impl Command {
    pub fn parse_with_reader<R: std::io::BufRead>(reader: &mut R) -> anyhow::Result<Command> {
        let data_type =
            DataType::parse(reader).context("Unable to read DataType to process command")?;
        Self::parse(data_type)
    }
    pub fn parse(data_type: DataType) -> anyhow::Result<Command> {
        match data_type {
            DataType::Array(items) => {
                if items.len() == 0 {
                    return Err(anyhow::anyhow!("Array must have at least one item"));
                }
                Self::from(&items[0], &items[1..])
            }
            DataType::Noop => Ok(Command::Noop),
            _ => Err(anyhow::anyhow!("Command must be of type Array")),
        }
    }
    fn from(command: &DataType, args: &[DataType]) -> anyhow::Result<Command> {
        let command = match command {
            DataType::BulkString(s) => s,
            _ => return Err(anyhow::anyhow!("Command must be of type BulkString")),
        };
        match command.as_ref() {
            "ping" => {
                if args.len() > 1 {
                    return Err(anyhow::anyhow!(
                        "Ping command must have at most one argument"
                    ));
                }

                match args.get(0) {
                    None => Ok(Command::Ping(None)),
                    Some(DataType::BulkString(value)) => Ok(Command::Ping(Some(value.to_owned()))),
                    _ => Err(anyhow::anyhow!("Ping args must be bulk string or empty")),
                }
            }
            "echo" => {
                if args.len() > 1 || args.len() == 0 {
                    return Err(anyhow::anyhow!(
                        "echo command must have exactly one argument"
                    ));
                }
                match args.get(0) {
                    Some(DataType::BulkString(value)) => Ok(Command::Echo(value.to_owned())),
                    _ => Err(anyhow::anyhow!("Echo args must be bulk string")),
                }
            }
            "set" => {
                if args.len() < 2 {
                    return Err(anyhow::anyhow!(
                        "Set command must have at least two arguments"
                    ));
                }
                let key = match args.get(0) {
                    Some(DataType::BulkString(key)) => key,
                    _ => return Err(anyhow::anyhow!("Key must be of type BulkString")),
                };
                let value = match args.get(1) {
                    Some(DataType::BulkString(value)) => value,
                    _ => return Err(anyhow::anyhow!("Value must be of type BulkString")),
                };
                Ok(Command::Set(key.to_owned(), value.to_owned()))
            }
            "get" => {
                if args.len() < 1 {
                    return Err(anyhow::anyhow!(
                        "Get command must have at least one argument"
                    ));
                }
                let key = match args.get(0) {
                    Some(DataType::BulkString(key)) => key,
                    _ => return Err(anyhow::anyhow!("Key must be of type BulkString")),
                };
                Ok(Command::Get(key.to_owned()))
            }
            _ => Err(anyhow::anyhow!("Unknown command")),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::command::Command;
    use crate::resp_parser::DataType;

    #[test]
    fn test_parse_with_reader() {
        use std::io::Cursor;
        let mut cursor: Cursor<&str> = Cursor::new("*2\r\n$4\r\necho\r\n$3\r\nhey\r\n");
        let command = Command::parse_with_reader(&mut cursor).expect("Unable to parse command");
        assert_eq!(command, Command::Echo("hey".to_string()));
    }
    #[test]
    fn test_parse() {
        struct Test {
            input: DataType,
            expected: Command,
        }
        let tests = vec![
            Test {
                input: DataType::Array(vec![
                    DataType::BulkString("echo".to_string()),
                    DataType::BulkString("hey".to_string()),
                ]),
                expected: Command::Echo("hey".to_string()),
            },
            Test {
                input: DataType::Array(vec![
                    DataType::BulkString("set".into()),
                    DataType::BulkString("key".into()),
                    DataType::BulkString("value".into()),
                ]),
                expected: Command::Set("key".into(), "value".into()),
            },
            Test {
                input: DataType::Array(vec![
                    DataType::BulkString("get".into()),
                    DataType::BulkString("key".into()),
                ]),
                expected: Command::Get("key".into()),
            },
        ];

        for test in tests {
            let command = Command::parse(test.input).expect("Unable to parse command");
            assert_eq!(command, test.expected);
        }
    }
}
