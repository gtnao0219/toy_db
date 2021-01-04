use crate::catalog::ColumnType;
use crate::value::Value;

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
pub struct CreateTableStmtAst {
    pub table_name: String,
    pub table_element_list: Vec<TableElementAst>,
}
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
pub struct TableElementAst {
    pub column_name: String,
    pub column_type: ColumnType,
}
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
pub struct InsertStmtAst {
    pub table_name: String,
    pub values: Vec<Value>,
}
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
pub struct SelectStmtAst {
    pub table_name: String,
}
