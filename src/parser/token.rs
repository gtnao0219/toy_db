use std::iter::Peekable;
use std::str::Chars;

#[derive(Debug, Clone, PartialEq, PartialOrd, Hash)]
pub enum Token {
  Ident(String),
  Integer(i64),
  String(String),
  Asterisk,
  Semicolon,
  Comma,
  LeftParen,
  RightParen,
  KeywordCreate,
  KeywordTable,
  KeywordInsert,
  KeywordInto,
  KeywordValues,
  KeywordSelect,
  KeywordFrom,
  KeywordInt,
  KeywordVarchar,
  EOF,
}

pub fn tokenize(iter: &mut Peekable<Chars>) -> Result<Vec<Token>, String> {
  let mut tokens = Vec::new();
  loop {
    match iter.peek() {
      Some(c) if c.is_whitespace() => {
        iter.next();
      }
      Some(c) if '_' == *c || c.is_alphabetic() => {
        let mut ret = String::new();
        loop {
          match iter.peek() {
            Some(cc) if '_' == *cc || cc.is_digit(10) || cc.is_alphabetic() => {
              ret = format!("{}{}", ret, cc.to_string());
              iter.next();
            }
            _ => {
              break;
            }
          }
        }
        tokens.push(match ret.to_uppercase().as_str() {
          "CREATE" => Token::KeywordCreate,
          "TABLE" => Token::KeywordTable,
          "INSERT" => Token::KeywordInsert,
          "INTO" => Token::KeywordInto,
          "VALUES" => Token::KeywordValues,
          "SELECT" => Token::KeywordSelect,
          "FROM" => Token::KeywordFrom,
          "INT" => Token::KeywordInt,
          "VARCHAR" => Token::KeywordVarchar,
          _ => Token::Ident(ret),
        })
      }
      Some(c) if vec![',', '(', ')', '*', ';'].contains(c) => {
        tokens.push(match *c {
          ',' => Token::Comma,
          '(' => Token::LeftParen,
          ')' => Token::RightParen,
          '*' => Token::Asterisk,
          ';' => Token::Semicolon,
          _ => Token::EOF,
        });
        iter.next();
      }
      Some(c) if c.is_digit(10) => {
        let mut ret = String::new();
        loop {
          match iter.peek() {
            Some(cc) if cc.is_digit(10) => {
              ret = format!("{}{}", ret, cc.to_string());
              iter.next();
            }
            _ => {
              break;
            }
          }
        }
        if let Ok(v) = ret.parse() {
          tokens.push(Token::Integer(v));
        } else {
          return Err(format!("failed convert: {}", ret));
        }
      }
      Some('\'') => {
        let mut ret = String::new();
        iter.next();
        loop {
          match iter.peek() {
            Some(c) if '\'' == *c => {
              iter.next();
              break;
            }
            Some(c) if '\\' == *c => {
              iter.next();
              match iter.peek() {
                Some(cc) if '\'' == *cc => {
                  ret = format!("{}{}", ret, cc.to_string());
                }
                _ => {
                  return Err(format!("invalid string literal: {}", ret));
                }
              }
            }
            Some(c) => {
              ret = format!("{}{}", ret, c.to_string());
              iter.next();
            }
            _ => {
              return Err(format!("invalid string literal: {}", ret));
            }
          }
        }
        tokens.push(Token::String(ret));
      }
      Some(c) => {
        return Err(format!("invalid token: {}", c))
      }
      None => {
        tokens.push(Token::EOF);
        break;
      }
    }
  }
  Ok(tokens)
}


#[cfg(test)]
mod tests {
  use crate::parser::token::{tokenize, Token};
  #[test]
  fn select_query() {
    let sql = "
    SELECT
      *
    FROM users;
    ";
    assert_eq!(
      tokenize(&mut sql.chars().peekable()),
      Ok(vec![
        Token::KeywordSelect,
        Token::Asterisk,
        Token::KeywordFrom, Token::Ident("users".to_string()),
        Token::Semicolon, Token::EOF,
      ])
    );
  }
  #[test]
  fn create_table_query() {
    let sql = "
    CREATE TABLE users
    (
      id Int,
      name Varchar
    );
    ";
    assert_eq!(
      tokenize(&mut sql.chars().peekable()),
      Ok(vec![
        Token::KeywordCreate, Token::KeywordTable, Token::Ident("users".to_string()),
        Token::LeftParen,
        Token::Ident("id".to_string()), Token::KeywordInt, Token::Comma,
        Token::Ident("name".to_string()), Token::KeywordVarchar,
        Token::RightParen,
        Token::Semicolon, Token::EOF,
      ])
    );
  }
  #[test]
  fn insert_query() {
    let sql = "
    INSERT INTO users
    VALUES
    (
      1,
      'foo'
    );
    ";
    assert_eq!(
      tokenize(&mut sql.chars().peekable()),
      Ok(vec![
        Token::KeywordInsert, Token::KeywordInto, Token::Ident("users".to_string()),
        Token::KeywordValues,
        Token::LeftParen,
        Token::Integer(1), Token::Comma,
        Token::String("foo".to_string()),
        Token::RightParen,
        Token::Semicolon, Token::EOF,
      ])
    );
  }
}