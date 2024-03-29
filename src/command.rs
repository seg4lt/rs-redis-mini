use std::time::Duration;

use anyhow::{anyhow, Context, Ok};

use crate::resp_parser::DataType;

#[derive(Debug, PartialEq)]
pub enum Command {
    Ping(Option<String>),
    Echo(String),
    Set(String, String, bool, Option<Duration>),
    Get(String),
    Info(Option<String>),
    Noop,
}

impl Command {
    pub fn parse_with_reader<R: std::io::BufRead>(reader: &mut R) -> anyhow::Result<Command> {
        let data_type =
            DataType::parse(reader).context("Unable to read DataType to process command")?;
        Self::parse(data_type)
    }
    pub fn parse(data_type: DataType) -> anyhow::Result<Command> {
        println!("🙏 >>> Command Request: {:?} <<<", data_type.to_string());
        match data_type {
            DataType::Array(items) => {
                if items.len() == 0 {
                    return Err(anyhow!("Array must have at least one item"));
                }
                Self::from(&items[0], &items[1..])
            }
            DataType::Noop => Ok(Command::Noop),
            _ => Err(anyhow!("Command must be of type Array")),
        }
    }
    fn from(command: &DataType, args: &[DataType]) -> anyhow::Result<Command> {
        let command = match command {
            DataType::BulkString(s) => s,
            _ => return Err(anyhow!("Command must be of type BulkString")),
        };
        match command.to_lowercase().as_ref() {
            "ping" => Self::parse_ping_cmd(args),
            "echo" => Self::parse_echo_cmd(args),
            "set" => Self::parse_set_cmd(args),
            "get" => Self::parse_get_cmd(args),
            "info" => Self::parse_info_cmd(args),
            _ => Err(anyhow!("Unknown command")),
        }
    }
    fn parse_info_cmd(args: &[DataType]) -> anyhow::Result<Command> {
        match args.get(0) {
            None => Ok(Command::Info(None)),
            Some(DataType::BulkString(value)) => Ok(Command::Info(Some(value.to_owned()))),
            _ => Err(anyhow!("Info args must be bulk string or empty")),
        }
    }
    fn parse_get_cmd(args: &[DataType]) -> anyhow::Result<Command> {
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
    fn parse_echo_cmd(args: &[DataType]) -> anyhow::Result<Command> {
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

    fn parse_ping_cmd(args: &[DataType]) -> anyhow::Result<Command> {
        if args.len() > 1 {
            return Err(anyhow!("Ping command must have at most one argument"));
        }
        match args.get(0) {
            None => Ok(Command::Ping(None)),
            Some(DataType::BulkString(value)) => Ok(Command::Ping(Some(value.to_owned()))),
            _ => Err(anyhow!("Ping args must be bulk string or empty")),
        }
    }
    fn parse_set_cmd(args: &[DataType]) -> anyhow::Result<Command> {
        if args.len() < 2 {
            return Err(anyhow!("Set command must have at least two arguments"));
        }
        let key = match args.get(0) {
            Some(DataType::BulkString(key)) => key,
            _ => return Err(anyhow!("Key must be of type BulkString")),
        };
        let value = match args.get(1) {
            Some(DataType::BulkString(value)) => value,
            _ => return Err(anyhow!("Value must be of type BulkString")),
        };
        let mut do_get = false;
        let mut exp_time: Option<Duration> = None;
        let mut i = 2;
        while i < args.len() {
            match args.get(i) {
                Some(DataType::BulkString(flag)) => match flag.to_lowercase().as_str() {
                    "get" => {
                        do_get = true;
                    }
                    "ex" | "px" => {
                        i += 1;
                        exp_time = Self::parse_set_cmd_exp_time_flag(
                            flag.to_lowercase().as_str(),
                            i,
                            args,
                        )?;
                    }
                    _ => Err(anyhow!("Unknown flag sent to SET command"))?,
                },
                _ => Err(anyhow!("Flag must be of type BulkString"))?,
            }
            i += 1;
        }
        return Ok(Command::Set(
            key.to_owned(),
            value.to_owned(),
            do_get,
            exp_time,
        ));
    }
    fn parse_set_cmd_exp_time_flag(
        flag: &str,
        value_idx: usize,
        args: &[DataType],
    ) -> anyhow::Result<Option<Duration>> {
        let value = args
            .get(value_idx)
            .ok_or_else(|| anyhow!(format!("{} flag must have a value", flag)))?;
        match value {
            DataType::BulkString(value) => {
                let value = value.parse::<u64>().context("Unable to parse value")?;
                match flag {
                    "px" => Ok(Some(Duration::from_millis(value))),
                    "ex" => Ok(Some(Duration::from_secs(value))),
                    _ => Err(anyhow!("Unknown flag for SET command")),
                }
            }
            _ => Err(anyhow!("Ex flag value must be a bulk string"))?,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

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
                expected: Command::Set("key".into(), "value".into(), false, None),
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
                    false,
                    Some(Duration::from_secs(1000)),
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
