use std::collections::HashMap;

use anyhow::{anyhow, bail, Context, Ok};

use crate::{fdbg, resp_parser::DataType};

#[derive(Debug, PartialEq, Clone)]
pub enum Command {
    Ping(Option<String>),
    Echo(String),
    // key, value, extra flags
    Set(String, String, Option<HashMap<String, String>>),
    Get(String),
    Info(Option<String>),
    ReplConf(String, String),
    PSync(String, String),
    Noop(String),
    ConnectionClosed,
}

impl Command {
    pub fn parse_with_reader<R: std::io::BufRead>(reader: &mut R) -> anyhow::Result<Command> {
        let data_type =
            DataType::parse(reader).context(fdbg!("Unable to read DataType to process command"))?;
        Self::parse(data_type)
    }
    pub fn parse(data_type: DataType) -> anyhow::Result<Command> {
        match data_type {
            DataType::EmptyString => Ok(Command::ConnectionClosed),
            DataType::Array(items) => {
                if items.len() == 0 {
                    bail!("Array must have at least one item");
                }
                Self::from(&items[0], &items[1..])
            }
            DataType::SimpleString(_) | DataType::BulkString(_) | DataType::NullBulkString => {
                Ok(Command::Noop(String::from_utf8(data_type.as_bytes())?))
            }
            DataType::NewLine(ch) => Ok(Command::Noop(format!("{}", ch))),
            DataType::RDSFile(_) => Ok(Command::Noop("RDSFile".into())),
        }
    }
    fn from(command: &DataType, args: &[DataType]) -> anyhow::Result<Command> {
        let command = match command {
            DataType::BulkString(s) => s,
            _ => bail!("Command must be of type BulkString"),
        };
        match command.to_lowercase().as_ref() {
            "ping" => Self::parse_ping_cmd(args),
            "echo" => Self::parse_echo_cmd(args),
            "set" => Self::parse_set_cmd(args),
            "get" => Self::parse_get_cmd(args),
            "info" => Self::parse_info_cmd(args),
            "replconf" => Self::parse_replconf_cmd(args),
            "psync" => Self::parse_psync_cmd(args),
            foo => bail!("Unknown command - {foo}"),
        }
    }
    fn parse_psync_cmd(args: &[DataType]) -> anyhow::Result<Command> {
        if args.len() < 2 {
            bail!("PSYNC command must have at least two arguments");
        }
        let key = match args.get(0) {
            Some(DataType::BulkString(key)) => key,
            _ => bail!("Key must be of type BulkString"),
        };
        let value = match args.get(1) {
            Some(DataType::BulkString(value)) => value,
            _ => bail!("Value must be of type BulkString"),
        };
        Ok(Command::PSync(key.to_owned(), value.to_owned()))
    }
    fn parse_replconf_cmd(args: &[DataType]) -> anyhow::Result<Command> {
        if args.len() < 2 {
            bail!("ReplConf command must have at least two arguments");
        }
        let key = match args.get(0) {
            Some(DataType::BulkString(key)) => key,
            _ => bail!("Key must be of type BulkString"),
        };
        let value = match args.get(1) {
            Some(DataType::BulkString(value)) => value,
            _ => bail!("Value must be of type BulkString"),
        };
        Ok(Command::ReplConf(key.to_owned(), value.to_owned()))
    }
    fn parse_info_cmd(args: &[DataType]) -> anyhow::Result<Command> {
        match args.get(0) {
            None => Ok(Command::Info(None)),
            Some(DataType::BulkString(value)) => Ok(Command::Info(Some(value.to_owned()))),
            _ => bail!("Info args must be bulk string or empty"),
        }
    }
    fn parse_get_cmd(args: &[DataType]) -> anyhow::Result<Command> {
        if args.len() < 1 {
            bail!("Get command must have at least one argument");
        }
        let key = match args.get(0) {
            Some(DataType::BulkString(key)) => key,
            _ => bail!("Key must be of type BulkString"),
        };
        Ok(Command::Get(key.to_owned()))
    }
    fn parse_echo_cmd(args: &[DataType]) -> anyhow::Result<Command> {
        if args.len() > 1 || args.len() == 0 {
            bail!("echo command must have exactly one argument");
        }
        match args.get(0) {
            Some(DataType::BulkString(value)) => Ok(Command::Echo(value.to_owned())),
            _ => bail!("Echo args must be bulk string"),
        }
    }
    fn parse_ping_cmd(args: &[DataType]) -> anyhow::Result<Command> {
        if args.len() > 1 {
            bail!("Ping command must have at most one argument");
        }
        match args.get(0) {
            None => Ok(Command::Ping(None)),
            Some(DataType::BulkString(value)) => Ok(Command::Ping(Some(value.to_owned()))),
            _ => bail!("Ping args must be bulk string or empty"),
        }
    }
    fn parse_set_cmd(args: &[DataType]) -> anyhow::Result<Command> {
        if args.len() < 2 {
            bail!("Set command must have at least two arguments");
        }
        let key = match args.get(0) {
            Some(DataType::BulkString(key)) => key,
            _ => bail!("Key must be of type BulkString"),
        };
        let value = match args.get(1) {
            Some(DataType::BulkString(value)) => value,
            _ => bail!("Value must be of type BulkString"),
        };
        let mut extra_flags: HashMap<String, String> = HashMap::new();
        let mut i = 2;
        while i < args.len() {
            match args.get(i) {
                Some(DataType::BulkString(flag)) => match flag.to_lowercase().as_str() {
                    "get" => {
                        extra_flags.insert("get".into(), "true".into());
                    }
                    "ex" | "px" => {
                        i += 1;
                        let value = args
                            .get(i)
                            .ok_or_else(|| anyhow!("Flag must have a value"))?;
                        let DataType::BulkString(value) = value else {
                            bail!("Flag must be of type BulkString");
                        };
                        extra_flags.insert(flag.to_lowercase(), value.clone());
                    }
                    _ => bail!("Unknown flag sent to SET command"),
                },
                _ => bail!("Flag must be of type BulkString"),
            }
            i += 1;
        }
        let extra_flags = match extra_flags.len() == 0 {
            true => None,
            false => Some(extra_flags),
        };
        return Ok(Command::Set(key.to_owned(), value.to_owned(), extra_flags));
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
                expected: Command::Set("key".into(), "value".into(), None),
            },
            Test {
                input: DataType::Array(vec![
                    DataType::BulkString("get".into()),
                    DataType::BulkString("key".into()),
                ]),
                expected: Command::Get("key".into()),
            },
            Test {
                input: DataType::Array(vec![
                    DataType::BulkString("set".into()),
                    DataType::BulkString("key".into()),
                    DataType::BulkString("value".into()),
                    DataType::BulkString("ex".into()),
                    DataType::BulkString("1000".into()),
                ]),
                expected: Command::Set(
                    "key".into(),
                    "value".into(),
                    Some(vec![("ex".into(), "1000".into())].into_iter().collect()),
                ),
            },
            Test {
                input: DataType::Array(vec![
                    DataType::BulkString("info".into()),
                    DataType::BulkString("replication".into()),
                ]),
                expected: Command::Info(Some("replication".into())),
            },
        ];

        for test in tests {
            let command = Command::parse(test.input).expect("Unable to parse command");
            assert_eq!(command, test.expected);
        }
    }
}
