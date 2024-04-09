use crate::{LINE_ENDING, NEW_LINE};

pub(crate) mod parser;

use RESPType::*;

#[derive(Debug, Clone)]
pub enum RESPType {
    Array(Vec<RESPType>),
    BulkString(String),
    NullBulkString,
    RDB(Vec<u8>),
    SimpleString(String),
    CustomNewLine,
    EOF,
}

impl RESPType {
    pub fn as_bytes(&self) -> Vec<u8> {
        match self {
            Array(array) => {
                let mut result = vec![b'*'];
                result.extend(array.len().to_string().as_bytes());
                result.extend(LINE_ENDING.as_bytes().to_vec());
                for resp_type in array {
                    result.extend(resp_type.as_bytes());
                }
                result
            }
            BulkString(string) => {
                let mut result = vec![b'$'];
                result.extend(string.len().to_string().as_bytes());
                result.extend(LINE_ENDING.as_bytes().to_vec());
                result.extend(string.as_bytes());
                result.extend(LINE_ENDING.as_bytes().to_vec());
                result
            }
            NullBulkString => {
                let mut result = vec![b'$', b'-', b'1'];
                result.extend(LINE_ENDING.as_bytes().to_vec());
                result
            }
            SimpleString(string) => {
                let mut result = vec![b'+'];
                result.extend(string.as_bytes());
                result.extend(LINE_ENDING.as_bytes().to_vec());
                result
            }
            RDB(data) => {
                let mut result = vec![b'$'];
                result.extend(data.len().to_string().as_bytes());
                result.extend(LINE_ENDING.as_bytes().to_vec());
                result.extend(data);
                result
            }
            CustomNewLine | RESPType::EOF => {
                let result = vec![NEW_LINE];
                result
            }
        }
    }
}
