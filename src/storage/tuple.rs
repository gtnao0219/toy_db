use std::io::Write;

use anyhow::Result;

use crate::catalog::Schema;
use crate::value::Value;

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
pub struct Tuple {
    pub values: Vec<Value>,
}

impl Tuple {
    pub fn serialize(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        for value in self.values.iter() {
            buf.write_all(&value.serialize())?;
        }
        Ok(buf)
    }
    pub fn deserialize(data: &[u8], schema: &Schema) -> Result<Tuple> {
        let mut values = Vec::new();
        let mut position = 0;
        for column in schema.columns.iter() {
            let value_and_size = Value::deserialize(&data[position..], &column.column_type)?;
            values.push(value_and_size.0);
            position += value_and_size.1;
        }
        Ok(Tuple { values })
    }
}

#[cfg(test)]
mod tests {
    use crate::catalog::{Column, ColumnType, Schema};
    use crate::storage::tuple::Tuple;
    use crate::value::Value;
    use std::i32;
    #[test]
    fn serialize() {
        let tuple = Tuple {
            values: vec![Value::Int(i32::MIN), Value::Varchar("foo".to_string())],
        };
        let b = tuple.serialize().unwrap();
        assert_eq!(b, vec![128, 0, 0, 0, 0, 0, 0, 3, 102, 111, 111]);
    }
    #[test]
    fn deserialize() {
        let tuple = Tuple::deserialize(
            &[128, 0, 0, 0, 0, 0, 0, 3, 102, 111, 111],
            &Schema {
                columns: vec![
                    Column {
                        name: "_1".to_string(),
                        column_type: ColumnType::Int,
                    },
                    Column {
                        name: "_2".to_string(),
                        column_type: ColumnType::Varchar,
                    },
                ],
            },
        )
        .unwrap();
        assert_eq!(
            tuple,
            Tuple {
                values: vec![Value::Int(i32::MIN), Value::Varchar("foo".to_string())]
            }
        );
    }
}
