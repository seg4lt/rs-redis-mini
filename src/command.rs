use anyhow::{Context, Ok};

use crate::resp_parser::DataType;

#[derive(Debug, PartialEq)]
pub enum Command {
    Ping(Option<String>),
    Echo(String),
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
                if let Some(DataType::BulkString(value)) = args.get(0) {
                    return Ok(Command::Ping(Some(value.to_owned())));
                }
                Ok(Command::Ping(None))
            }
            "echo" => {
                if args.len() > 1 || args.len() == 0 {
                    return Err(anyhow::anyhow!(
                        "echo command must have exactly one argument"
                    ));
                }
                if let DataType::BulkString(value) = args.get(0).unwrap() {
                    return Ok(Command::Echo(value.to_owned()));
                };
                Err(anyhow::anyhow!(
                    "Echo command must have exactly one argument"
                ))
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
        let d_type = DataType::Array(vec![
            DataType::BulkString("echo".to_string()),
            DataType::BulkString("hey".to_string()),
        ]);
        let command = Command::parse(d_type).expect("Unable to parse command");
        assert_eq!(command, Command::Echo("hey".to_string()));
    }
}
