use std::fmt;
use std::i32;
use std::io;
use std::io::Read;

use crate::catalog::ColumnType;

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
pub enum Value {
    Int(i32),
    Varchar(String),
}

impl Value {
    pub fn serialize(&self) -> Vec<u8> {
        match self {
            Value::Int(v) => v.to_be_bytes().to_vec(),
            Value::Varchar(v) => {
                let str_byte = v.as_bytes();
                let str_size = str_byte.len() as u32;
                let str_size_byte = &str_size.to_be_bytes();
                [str_size_byte, str_byte].concat()
            }
        }
    }
    pub fn deserialize(data: &[u8], column_type: &ColumnType) -> io::Result<(Self, usize)> {
        let mut reader = &data[..];
        match column_type {
            ColumnType::Int => {
                let mut buf = [0u8; 4];
                reader.read_exact(&mut buf)?;
                let v = i32::from_be_bytes(buf);
                Ok((Value::Int(v), 4))
            }
            ColumnType::Varchar => {
                let mut buf = [0u8; 4];
                reader.read_exact(&mut buf)?;
                let size = u32::from_be_bytes(buf) as usize;
                let mut str_buf = vec![0u8; size];
                reader.read_exact(&mut str_buf)?;
                // TODO: remove unwrap
                Ok((
                    Value::Varchar(String::from_utf8(str_buf).unwrap()),
                    4usize + size,
                ))
            }
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self {
            Value::Int(v) => write!(f, "{}", v),
            Value::Varchar(v) => write!(f, "{}", v),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::catalog::ColumnType;
    use crate::value::Value;
    use std::i32;
    #[test]
    fn serialize_int_zero() {
        assert_eq!(Value::Int(0).serialize(), vec![0, 0, 0, 0]);
    }
    #[test]
    fn serialize_int_min() {
        assert_eq!(Value::Int(i32::MIN).serialize(), vec![128, 0, 0, 0]);
    }
    #[test]
    fn serialize_int_max() {
        assert_eq!(Value::Int(i32::MAX).serialize(), vec![127, 255, 255, 255]);
    }
    #[test]
    fn serialize_varchar_jp() {
        assert_eq!(
            Value::Varchar("あいうえお".to_string()).serialize(),
            vec![
                0, 0, 0, 15, 227, 129, 130, 227, 129, 132, 227, 129, 134, 227, 129, 136, 227, 129,
                138
            ]
        )
    }
    #[test]
    fn deseriallize_int_zero() {
        let value = Value::deserialize(&[0, 0, 0, 0], &ColumnType::Int).unwrap();
        assert_eq!(value, (Value::Int(0), 4))
    }
    #[test]
    fn deseriallize_int_min() {
        let value = Value::deserialize(&[128, 0, 0, 0], &ColumnType::Int).unwrap();
        assert_eq!(value, (Value::Int(i32::MIN), 4))
    }
    #[test]
    fn deseriallize_int_max() {
        let value = Value::deserialize(&[127, 255, 255, 255], &ColumnType::Int).unwrap();
        assert_eq!(value, (Value::Int(i32::MAX), 4))
    }
    #[test]
    fn deserialize_varchar_jp() {
        let value = Value::deserialize(
            &[
                0, 0, 0, 15, 227, 129, 130, 227, 129, 132, 227, 129, 134, 227, 129, 136, 227, 129,
                138,
            ],
            &ColumnType::Varchar,
        )
        .unwrap();
        assert_eq!(value, (Value::Varchar("あいうえお".to_string()), 19))
    }
}
