use std::{fmt::Display, iter::Peekable, str::Chars};

use crate::error::{Error, Result};

#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    Keyword(Keyword),
    // string tokens, e.g. table, column
    Ident(String),
    String(String),
    Number(String),
    OpenParen, // (
    CloseParen, // )
    Comma, 
    Semicolon,
    Asterisk, // *
    Plus,
    Minus,
    Slash,
}

impl Display for Token {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Token::Keyword(keyword) => keyword.to_str(),
            Token::Ident(ident) => ident,
            Token::String(v) => v,
            Token::Number(n) => n,
            Token::OpenParen => "(",
            Token::CloseParen => ")",
            Token::Comma => ",",
            Token::Semicolon => ";",
            Token::Asterisk => "*",
            Token::Plus => "+",
            Token::Minus => "-",
            Token::Slash => "/",
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Keyword {
    Create,
    Table,
    Int,
    Integer,
    Boolean,
    Bool,
    String,
    Text,
    Varchar,
    Float,
    Double,
    Select,
    From,
    Insert,
    Into,
    Values,
    True,
    False,
    Default,
    Not,
    Null,
    Primary,
    Key,
}

impl Keyword {
    pub fn from_str(ident: &str) -> Option<Self> {
        Some(match ident.to_uppercase().as_ref() {
            "CREATE" => Keyword::Create,
            "TABLE" => Keyword::Table,
            "INT" => Keyword::Int,
            "INTEGER" => Keyword::Integer,
            "BOOLEAN" => Keyword::Boolean,
            "BOOL" => Keyword::Bool,
            "STRING" => Keyword::String,
            "TEXT" => Keyword::Text,
            "VARCHAR" => Keyword::Varchar,
            "FLOAT" => Keyword::Float,
            "DOUBLE" => Keyword::Double,
            "SELECT" => Keyword::Select,
            "FROM" => Keyword::From,
            "INSERT" => Keyword::Insert,
            "INTO" => Keyword::Into,
            "VALUES" => Keyword::Values,
            "TRUE" => Keyword::True,
            "FALSE" => Keyword::False,
            "DEFAULT" => Keyword::Default,
            "NOT" => Keyword::Not,
            "NULL" => Keyword::Null,
            "PRIMARY" => Keyword::Primary,
            "KEY" => Keyword::Key,
            _ => return None,
        })
    }

    pub fn to_str(&self) -> &str {
        match self {
            Keyword::Create => "CREATE",
            Keyword::Table => "TABLE",
            Keyword::Int => "INT",
            Keyword::Integer => "INTEGER",
            Keyword::Boolean => "BOOLEAN",
            Keyword::Bool => "BOOL",
            Keyword::String => "STRING",
            Keyword::Text => "TEXT",
            Keyword::Varchar => "VARCHAR",
            Keyword::Float => "FLOAT",
            Keyword::Double => "DOUBLE",
            Keyword::Select => "SELECT",
            Keyword::From => "FROM",
            Keyword::Insert => "INSERT",
            Keyword::Into => "INTO",
            Keyword::Values => "VALUES",
            Keyword::True => "TRUE",
            Keyword::False => "FALSE",
            Keyword::Default => "DEFAULT",
            Keyword::Not => "NOT",
            Keyword::Null => "NULL",
            Keyword::Primary => "PRIMARY",
            Keyword::Key => "KEY",
        }
    }
}

impl Display for Keyword {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.to_str())
    }
}

// 1. Create Table
// -------------------------------------
// CREATE TABLE table_name (
//     [ column_name data_type [ column_constraint [...] ] ]
//     [, ... ]
//    );
//
//    where data_type is:
//     - BOOLEAN(BOOL): true | false
//     - FLOAT(DOUBLE)
//     - INTEGER(INT)
//     - STRING(TEXT, VARCHAR)
//
//    where column_constraint is:
//    [ NOT NULL | NULL | DEFAULT expr ]
//
// 2. Insert Into
// -------------------------------------
// INSERT INTO table_name
// [ ( column_name [, ...] ) ]
// values ( expr [, ...] );
// 3. Select * From
// -------------------------------------
// SELECT * FROM table_name;
pub struct Lexer<'a> {
    iter: Peekable<Chars<'a>>,
}

// supported SQL command
impl<'a> Iterator for Lexer<'a> {
    type Item = Result<Token>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.scan() {
            Ok(Some(token)) => Some(Ok(token)),
            Ok(None) => self
                .iter
                .peek()
                .map(|c| Err(Error::Parse(format!("[Lexer] Unexpeted character {}", c)))),
            Err(err) => Some(Err(err)),
        }
    }
}

impl<'a> Lexer<'a> {
    pub fn new(sql_text: &'a str) -> Self {
        Self {
            iter: sql_text.chars().peekable(),
        }
    }

    // remove blank char
    // eg. selct *       from        t;
    fn erase_whitespace(&mut self) {
        self.next_while(|c| c.is_whitespace());
    }

    // jump to the next char and return the curr one
    fn next_if<F: Fn(char) -> bool>(&mut self, predicate: F) -> Option<char> {
        self.iter.peek().filter(|&c| predicate(*c))?;
        self.iter.next()
    }

    // if condition met jump to the next char
    fn next_while<F: Fn(char) -> bool>(&mut self, predicate: F) -> Option<String> {
        let mut value = String::new();
        while let Some(c) = self.next_if(&predicate) {
            value.push(c);
        }

        Some(value).filter(|v| !v.is_empty())
    }

    // if only Token: jump to the next and return token
    fn next_if_token<F: Fn(char) -> Option<Token>>(&mut self, predicate: F) -> Option<Token> {
        let token = self.iter.peek().and_then(|c| predicate(*c))?;
        self.iter.next();
        Some(token)
    }

// scan and get the next Token
fn scan(&mut self) -> Result<Option<Token>> {
    // remove blank
    self.erase_whitespace();
    // check condition based on the first letter
    match self.iter.peek() {
        Some('\'') => self.scan_string(), // scan strings
        Some(c) if c.is_ascii_digit() => Ok(self.scan_number()), // scan numbers
        Some(c) if c.is_alphabetic() => Ok(self.scan_ident()), // scan Ident
        Some(_) => Ok(self.scan_symbol()), // scan symbols
        None => Ok(None),
    }
}

fn scan_string(&mut self) -> Result<Option<Token>> {
    // 判断是否是单引号开头
    if self.next_if(|c| c == '\'').is_none() {
        return Ok(None);
    }

    let mut val = String::new();
    loop {
        match self.iter.next() {
            Some('\'') => break,
            Some(c) => val.push(c),
            None => return Err(Error::Parse(format!("[Lexer] Unexpected end of string"))),
        }
    }

    Ok(Some(Token::String(val)))
}

fn scan_number(&mut self) -> Option<Token> {
    // scan partially
    let mut num = self.next_while(|c| c.is_ascii_digit())?;
    // if dot appears, the number is float
    if let Some(sep) = self.next_if(|c| c == '.') {
        num.push(sep);
        // scan the part dehind dot
        while let Some(c) = self.next_if(|c| c.is_ascii_digit()) {
            num.push(c);
        }
    }

    Some(Token::Number(num))
}

// scan Ident: table/column names, or keywords
fn scan_ident(&mut self) -> Option<Token> {
    let mut value = self.next_if(|c| c.is_alphabetic())?.to_string();
    while let Some(c) = self.next_if(|c| c.is_alphanumeric() || c == '_') {
        value.push(c);
    }

    Some(Keyword::from_str(&value).map_or(Token::Ident(value.to_lowercase()), Token::Keyword))
}

// scan symbols
fn scan_symbol(&mut self) -> Option<Token> {
    self.next_if_token(|c| match c {
        '*' => Some(Token::Asterisk),
        '(' => Some(Token::OpenParen),
        ')' => Some(Token::CloseParen),
        ',' => Some(Token::Comma),
        ';' => Some(Token::Semicolon),
        '+' => Some(Token::Plus),
        '-' => Some(Token::Minus),
        '/' => Some(Token::Slash),
        _ => None,
    })
}
}
