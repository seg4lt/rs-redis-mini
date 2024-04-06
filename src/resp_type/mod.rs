use crate::{LINE_ENDING, NEW_LINE};

pub(crate) mod parser;

#[derive(Debug)]
pub enum RESPType {
    Array(Vec<RESPType>),
    BulkString(String),
    SimpleString(String),
    CustomNewLine,
    EOF,
}

impl RESPType {
    pub fn as_bytes(&self) -> Vec<u8> {
        match self {
            RESPType::Array(array) => {
                let mut result = vec![b'*'];
                result.extend(array.len().to_string().as_bytes());
                result.extend(LINE_ENDING);
                for resp_type in array {
                    result.extend(resp_type.as_bytes());
                }
                result
            }
            RESPType::BulkString(string) => {
                let mut result = vec![b'$'];
                result.extend(string.len().to_string().as_bytes());
                result.extend(LINE_ENDING);
                result.extend(string.as_bytes());
                result.extend(LINE_ENDING);
                result
            }
            RESPType::SimpleString(string) => {
                let mut result = vec![b'+'];
                result.extend(string.as_bytes());
                result.extend(LINE_ENDING);
                result
            }
            RESPType::CustomNewLine | RESPType::EOF => {
                let result = vec![NEW_LINE];
                result
            }
        }
    }
}
