use crate::value::Value;

#[derive(Debug, Clone, PartialEq, PartialOrd, Hash, Default, Eq, Ord)]
pub struct CreateTableStmtAst {
  pub table_name: String,
  pub table_element_list: Vec<TableElementAst>,
}
#[derive(Debug, Clone, PartialEq, PartialOrd, Hash, Eq, Ord)]
pub struct TableElementAst {
  pub column_name: String,
  pub column_type: ColumnTypeAst,
}
#[derive(Debug, Clone, PartialEq, PartialOrd, Hash, Eq, Ord)]
pub enum ColumnTypeAst {
  Int,
  Varchar,
}
#[derive(Debug, Clone, PartialEq, PartialOrd, Hash, Default, Eq, Ord)]
pub struct InsertStmtAst {
  pub table_name: String,
  pub values: Vec<Value>
}
#[derive(Debug, Clone, PartialEq, PartialOrd, Hash, Default, Eq, Ord)]
pub struct SelectStmtAst {
  pub table_name: String,
}
