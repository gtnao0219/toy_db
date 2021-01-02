use super::column::{Column};

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
pub struct Schema {
  pub columns: Vec<Column>,
}
