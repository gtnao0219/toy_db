pub mod ast;
pub mod token;

use anyhow::Result;

use self::token::Token;
use crate::catalog::ColumnType;
use crate::value::Value;

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
pub enum Stmt {
    CreateTableStmt(ast::CreateTableStmtAst),
    InsertStmt(ast::InsertStmtAst),
    SelectStmt(ast::SelectStmtAst),
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
pub struct Parser {
    tokens: Vec<Token>,
    position: usize,
}

impl Parser {
    pub fn new(tokens: Vec<Token>) -> Parser {
        Parser {
            tokens,
            position: 0,
        }
    }
    fn reset_position(&mut self) {
        self.position = 0;
    }
    fn consume_or_err(&mut self, token: Token) -> Result<(), String> {
        if token == self.tokens[self.position] {
            self.position += 1;
            Ok(())
        } else {
            Err(format!("expected {:?}", token))
        }
    }
    fn consume(&mut self, token: Token) -> bool {
        if token == self.tokens[self.position] {
            self.position += 1;
            true
        } else {
            false
        }
    }
    fn consume_ident_or_err(&mut self) -> Result<String, String> {
        if let Token::Ident(v) = &self.tokens[self.position] {
            self.position += 1;
            Ok(v.clone())
        } else {
            Err("expected identifier".to_string())
        }
    }
    fn consume_lit_or_err(&mut self) -> Result<Value, String> {
        if let Token::Lit(v) = &self.tokens[self.position] {
            self.position += 1;
            Ok(v.clone())
        } else {
            Err("expected value".to_string())
        }
    }
    pub fn parse(&mut self) -> Result<Stmt, String> {
        self.stmt()
    }
    fn stmt(&mut self) -> Result<Stmt, String> {
        if let Ok(ast) = self.create_table_stmt() {
            Ok(Stmt::CreateTableStmt(ast))
        } else if let Ok(ast) = self.insert_stmt() {
            Ok(Stmt::InsertStmt(ast))
        } else if let Ok(ast) = self.select_stmt() {
            Ok(Stmt::SelectStmt(ast))
        } else {
            Err("invalid query".to_string())
        }
    }
    fn create_table_stmt(&mut self) -> Result<ast::CreateTableStmtAst, String> {
        self.reset_position();
        self.consume_or_err(Token::KeywordCreate)?;
        self.consume_or_err(Token::KeywordTable)?;
        let table_name = self.consume_ident_or_err()?;
        let table_element_list = self.table_element_list()?;
        Ok(ast::CreateTableStmtAst {
            table_name,
            table_element_list,
        })
    }
    fn table_element_list(&mut self) -> Result<Vec<ast::TableElementAst>, String> {
        self.consume_or_err(Token::LeftParen)?;
        let mut ret: Vec<ast::TableElementAst> = Vec::new();
        let table_element = self.table_element()?;
        ret.push(table_element);
        loop {
            if self.consume(Token::Comma) {
                let table_element = self.table_element()?;
                ret.push(table_element);
            } else {
                break;
            }
        }
        self.consume_or_err(Token::RightParen)?;
        Ok(ret)
    }
    fn table_element(&mut self) -> Result<ast::TableElementAst, String> {
        let column_name = self.consume_ident_or_err()?;
        if self.consume(Token::KeywordInt) {
            Ok(ast::TableElementAst {
                column_name,
                column_type: ColumnType::Int,
            })
        } else if self.consume(Token::KeywordVarchar) {
            Ok(ast::TableElementAst {
                column_name,
                column_type: ColumnType::Varchar,
            })
        } else {
            Err("invalid column type".to_string())
        }
    }
    fn insert_stmt(&mut self) -> Result<ast::InsertStmtAst, String> {
        self.reset_position();
        self.consume_or_err(Token::KeywordInsert)?;
        self.consume_or_err(Token::KeywordInto)?;
        let table_name = self.consume_ident_or_err()?;
        let values = self.table_value_constructor()?;
        Ok(ast::InsertStmtAst { table_name, values })
    }
    fn table_value_constructor(&mut self) -> Result<Vec<Value>, String> {
        self.consume_or_err(Token::KeywordValues)?;
        self.consume_or_err(Token::LeftParen)?;
        let mut ret: Vec<Value> = Vec::new();
        let value = self.consume_lit_or_err()?;
        ret.push(value);
        loop {
            if self.consume(Token::Comma) {
                let value = self.consume_lit_or_err()?;
                ret.push(value);
            } else {
                break;
            }
        }
        self.consume_or_err(Token::RightParen)?;
        Ok(ret)
    }
    fn select_stmt(&mut self) -> Result<ast::SelectStmtAst, String> {
        self.reset_position();
        self.consume_or_err(Token::KeywordSelect)?;
        self.consume_or_err(Token::Asterisk)?;
        self.consume_or_err(Token::KeywordFrom)?;
        let table_name = self.consume_ident_or_err()?;
        Ok(ast::SelectStmtAst { table_name })
    }
}

#[cfg(test)]
mod tests {
    use crate::catalog::ColumnType;
    use crate::parser::ast;
    use crate::parser::token::Token;
    use crate::parser::{Parser, Stmt};
    use crate::value::Value;
    #[test]
    fn select_stmt() {
        let mut parser = Parser::new(vec![
            Token::KeywordSelect,
            Token::Asterisk,
            Token::KeywordFrom,
            Token::Ident("users".to_string()),
            Token::Semicolon,
            Token::EOF,
        ]);
        assert_eq!(
            parser.parse(),
            Ok(Stmt::SelectStmt(ast::SelectStmtAst {
                table_name: "users".to_string(),
            }))
        );
    }
    #[test]
    fn create_table_stmt() {
        let mut parser = Parser::new(vec![
            Token::KeywordCreate,
            Token::KeywordTable,
            Token::Ident("users".to_string()),
            Token::LeftParen,
            Token::Ident("id".to_string()),
            Token::KeywordInt,
            Token::Comma,
            Token::Ident("name".to_string()),
            Token::KeywordVarchar,
            Token::RightParen,
            Token::Semicolon,
            Token::EOF,
        ]);
        assert_eq!(
            parser.parse(),
            Ok(Stmt::CreateTableStmt(ast::CreateTableStmtAst {
                table_name: "users".to_string(),
                table_element_list: vec![
                    ast::TableElementAst {
                        column_name: "id".to_string(),
                        column_type: ColumnType::Int,
                    },
                    ast::TableElementAst {
                        column_name: "name".to_string(),
                        column_type: ColumnType::Varchar,
                    }
                ]
            }))
        );
    }
    #[test]
    fn insert_stmt() {
        let mut parser = Parser::new(vec![
            Token::KeywordInsert,
            Token::KeywordInto,
            Token::Ident("users".to_string()),
            Token::KeywordValues,
            Token::LeftParen,
            Token::Lit(Value::Int(1)),
            Token::Comma,
            Token::Lit(Value::Varchar("foo".to_string())),
            Token::RightParen,
            Token::Semicolon,
            Token::EOF,
        ]);
        assert_eq!(
            parser.parse(),
            Ok(Stmt::InsertStmt(ast::InsertStmtAst {
                table_name: "users".to_string(),
                values: vec![Value::Int(1), Value::Varchar("foo".to_string()),]
            }))
        );
    }
}
