#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
pub struct Column {
  pub name: String,
  pub column_type: ColumnType,
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
pub enum ColumnType {
  Integer,
  Varchar,
}
