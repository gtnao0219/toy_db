use std::io::{Read};

use crate::catalog::column::{ColumnType};

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
pub enum Value {
  Integer(i32),
  Varchar(String)
}

impl Value {
  pub fn serialize(&self) -> Vec<u8> {
    match self {
      Value::Integer(v) => {
        v.to_be_bytes().to_vec()
      }
      Value::Varchar(v) => {
        let str_byte = v.as_bytes();
        let str_size = str_byte.len() as u32;
        let str_size_byte = &str_size.to_be_bytes();
        [str_size_byte, str_byte].concat()
      }
    }
  }
  pub fn deserialize(data: &[u8], column_type: &ColumnType) -> Self {
    let mut reader = &data[..];
    match column_type {
      ColumnType::Integer => {
        let mut buf = [0u8; 4];
        reader.read_exact(&mut buf);
        let v = i32::from_be_bytes(buf);
        Value::Integer(v)
      }
      ColumnType::Varchar => {
        let mut buf = [0u8; 4];
        reader.read_exact(&mut buf);
        let size = u32::from_be_bytes(buf) as usize;
        let mut str_buf = vec![0u8; size];
        reader.read_exact(&mut str_buf);
        Value::Varchar(String::from_utf8(str_buf).unwrap())
      }
    }
  }
}

#[cfg(test)]
mod tests {
  use crate::value::{Value};
  use crate::catalog::column::{ColumnType};
  #[test]
  fn serialize() {
    assert_eq!(
      Value::Integer(0).serialize(),
      vec![
        0,0,0,0,0,0,0,0
      ]
    );
  }
  #[test]
  fn deseriallize_integer() {
    assert_eq!(
      Value::deserialize(&[0,0,0,0,0,0,0,0], &ColumnType::Integer),
      Value::Integer(0)
    )
  }
  // #[test]
  // fn deseriallize_varchar() {
  //   assert_eq!(
  //     Value::deserialize(&[0,0,0,0,0,0,0,0], &ColumnType::Varchar),
  //     Value::Varchar("foo")
  //   )
  // }
}