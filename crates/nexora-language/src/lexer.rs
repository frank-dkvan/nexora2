//! Cypher lexer — tokenizes input strings into Token streams.

use crate::token::Token;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum LexerError {
    #[error("unexpected character '{0}' at position {1}")]
    UnexpectedChar(char, usize),
    #[error("unterminated string starting at position {0}")]
    UnterminatedString(usize),
    #[error("invalid number at position {0}")]
    InvalidNumber(usize),
    #[error("unterminated block comment starting at position {0}")]
    UnterminatedComment(usize),
}

/// Cypher lexer.
pub struct Lexer {
    input: Vec<char>,
    pos: usize,
}

impl Lexer {
    pub fn new(input: &str) -> Self {
        Self {
            input: input.chars().collect(),
            pos: 0,
        }
    }

    /// Tokenize the entire input.
    pub fn tokenize(&mut self) -> Result<Vec<Token>, LexerError> {
        let mut tokens = Vec::new();
        loop {
            let tok = self.next_token()?;
            let is_eof = tok == Token::Eof;
            tokens.push(tok);
            if is_eof {
                break;
            }
        }
        Ok(tokens)
    }

    fn peek(&self) -> Option<char> {
        self.input.get(self.pos).copied()
    }

    fn advance(&mut self) -> Option<char> {
        let ch = self.input.get(self.pos).copied();
        self.pos += 1;
        ch
    }

    fn skip_whitespace_and_comments(&mut self) -> Result<(), LexerError> {
        loop {
            // Skip whitespace
            while self.peek().is_some_and(|c| c.is_whitespace()) {
                self.advance();
            }
            // Skip line comments (// or --)
            if self.pos + 1 < self.input.len() {
                let slice: String = self.input[self.pos..self.pos + 2].iter().collect();
                if slice == "//" || slice == "--" {
                    while self.peek().is_some_and(|c| c != '\n') {
                        self.advance();
                    }
                    continue;
                }
                // Skip block comments (/* ... */)
                if slice == "/*" {
                    let start = self.pos;
                    self.advance();
                    self.advance();
                    let mut terminated = false;
                    loop {
                        if self.pos + 1 >= self.input.len() {
                            break;
                        }
                        let peek_slice: String =
                            self.input[self.pos..self.pos + 2].iter().collect();
                        if peek_slice == "*/" {
                            self.advance();
                            self.advance();
                            terminated = true;
                            break;
                        }
                        self.advance();
                    }
                    if !terminated {
                        return Err(LexerError::UnterminatedComment(start));
                    }
                    continue;
                }
            }
            break;
        }
        Ok(())
    }

    fn next_token(&mut self) -> Result<Token, LexerError> {
        self.skip_whitespace_and_comments()?;

        let ch = match self.peek() {
            Some(c) => c,
            None => return Ok(Token::Eof),
        };

        match ch {
            '(' => {
                self.advance();
                Ok(Token::LParen)
            }
            ')' => {
                self.advance();
                Ok(Token::RParen)
            }
            '[' => {
                self.advance();
                Ok(Token::LBracket)
            }
            ']' => {
                self.advance();
                Ok(Token::RBracket)
            }
            '{' => {
                self.advance();
                Ok(Token::LBrace)
            }
            '}' => {
                self.advance();
                Ok(Token::RBrace)
            }
            ',' => {
                self.advance();
                Ok(Token::Comma)
            }
            ';' => {
                self.advance();
                Ok(Token::Semicolon)
            }
            ':' => {
                self.advance();
                Ok(Token::Colon)
            }
            '|' => {
                self.advance();
                Ok(Token::Pipe)
            }
            '%' => {
                self.advance();
                Ok(Token::Percent)
            }
            '~' => {
                self.advance();
                Ok(Token::Tilde)
            }
            '+' => {
                self.advance();
                if self.peek() == Some('=') {
                    self.advance();
                    Ok(Token::FatArrow)
                } else {
                    Ok(Token::Plus)
                }
            }
            '-' => {
                self.advance();
                if self.peek() == Some('>') {
                    self.advance();
                    Ok(Token::ArrowRight)
                } else {
                    Ok(Token::Minus)
                }
            }
            '<' => {
                self.advance();
                match self.peek() {
                    Some('=') => {
                        self.advance();
                        Ok(Token::Le)
                    }
                    Some('-') => {
                        self.advance();
                        Ok(Token::ArrowLeft)
                    }
                    Some('>') => {
                        self.advance();
                        Ok(Token::NotEquals)
                    }
                    _ => Ok(Token::Lt),
                }
            }
            '>' => {
                self.advance();
                if self.peek() == Some('=') {
                    self.advance();
                    Ok(Token::Ge)
                } else {
                    Ok(Token::Gt)
                }
            }
            '=' => {
                self.advance();
                if self.peek() == Some('=') {
                    self.advance();
                    Ok(Token::Equals)
                } else if self.peek() == Some('~') {
                    self.advance();
                    Ok(Token::RegexMatch)
                } else {
                    Ok(Token::Equals)
                }
            }
            '*' => {
                self.advance();
                Ok(Token::Star)
            }
            '/' => {
                self.advance();
                Ok(Token::Slash)
            }
            '.' => {
                self.advance();
                if self.peek() == Some('.') {
                    self.advance();
                    Ok(Token::DoubleDot)
                } else {
                    Ok(Token::Dot)
                }
            }
            '"' | '\'' => self.read_string(),
            '`' => self.read_escaped_identifier(),
            _ if ch.is_ascii_digit() => self.read_number(),
            _ if ch.is_ascii_alphabetic() || ch == '_' => self.read_identifier(),
            _ => {
                let pos = self.pos;
                self.advance();
                Err(LexerError::UnexpectedChar(ch, pos))
            }
        }
    }

    fn read_string(&mut self) -> Result<Token, LexerError> {
        let quote = self.advance().unwrap();
        let start = self.pos;
        let mut s = String::new();

        loop {
            match self.peek() {
                None => return Err(LexerError::UnterminatedString(start)),
                Some(c) if c == quote => {
                    self.advance();
                    return Ok(Token::StringLiteral(s));
                }
                Some('\\') => {
                    self.advance();
                    match self.advance() {
                        Some('n') => s.push('\n'),
                        Some('t') => s.push('\t'),
                        Some('r') => s.push('\r'),
                        Some('\\') => s.push('\\'),
                        Some(c) => s.push(c),
                        None => return Err(LexerError::UnterminatedString(start)),
                    }
                }
                Some(c) => {
                    self.advance();
                    s.push(c);
                }
            }
        }
    }

    fn read_escaped_identifier(&mut self) -> Result<Token, LexerError> {
        self.advance(); // skip opening backtick
        let mut s = String::new();
        loop {
            match self.peek() {
                None => return Err(LexerError::UnterminatedString(self.pos)),
                Some('`') => {
                    self.advance();
                    return Ok(Token::EscapedIdentifier(s));
                }
                Some(c) => {
                    self.advance();
                    s.push(c);
                }
            }
        }
    }

    fn read_number(&mut self) -> Result<Token, LexerError> {
        let start = self.pos;
        let mut s = String::new();
        let mut is_float = false;

        // Optional negative sign
        // (already consumed by caller if '-')

        // Integer part
        while self.peek().is_some_and(|c| c.is_ascii_digit()) {
            s.push(self.advance().unwrap());
        }

        // Decimal part
        if self.peek() == Some('.') {
            // Check for double-dot (..)
            if self.pos + 1 < self.input.len() && self.input[self.pos + 1] == '.' {
                // Not a float, just an integer before `..`
            } else {
                is_float = true;
                s.push(self.advance().unwrap());
                while self.peek().is_some_and(|c| c.is_ascii_digit()) {
                    s.push(self.advance().unwrap());
                }
            }
        }

        // Exponent part
        if self.peek().is_some_and(|c| c == 'e' || c == 'E') {
            is_float = true;
            s.push(self.advance().unwrap());
            if self.peek().is_some_and(|c| c == '+' || c == '-') {
                s.push(self.advance().unwrap());
            }
            while self.peek().is_some_and(|c| c.is_ascii_digit()) {
                s.push(self.advance().unwrap());
            }
        }

        if is_float {
            s.parse::<f64>()
                .map(Token::FloatLiteral)
                .map_err(|_| LexerError::InvalidNumber(start))
        } else {
            s.parse::<i64>()
                .map(Token::IntegerLiteral)
                .map_err(|_| LexerError::InvalidNumber(start))
        }
    }

    fn read_identifier(&mut self) -> Result<Token, LexerError> {
        let mut s = String::new();
        while self
            .peek()
            .is_some_and(|c| c.is_ascii_alphanumeric() || c == '_')
        {
            s.push(self.advance().unwrap());
        }

        // Check if it's a keyword (case-insensitive)
        let upper = s.to_uppercase();
        let token = match upper.as_str() {
            "MATCH" => Token::Match,
            "OPTIONAL" => Token::Optional,
            "WHERE" => Token::Where,
            "CREATE" => Token::Create,
            "MERGE" => Token::Merge,
            "SET" => Token::Set,
            "REMOVE" => Token::Remove,
            "DELETE" => Token::Delete,
            "DETACH" => Token::Detach,
            "RETURN" => Token::Return,
            "WITH" => Token::With,
            "UNWIND" => Token::Unwind,
            "AS" => Token::As,
            "AND" => Token::And,
            "OR" => Token::Or,
            "XOR" => Token::Xor,
            "NOT" => Token::Not,
            "IN" => Token::In,
            "STARTS" => Token::StartsWith,
            "ENDS" => Token::EndsWith,
            "CONTAINS" => Token::Contains,
            "DISTINCT" => Token::Distinct,
            "ORDER" => Token::Order,
            "BY" => Token::By,
            "ASC" | "ASCENDING" => Token::Ascending,
            "DESC" | "DESCENDING" => Token::Descending,
            "SKIP" => Token::Skip,
            "LIMIT" => Token::Limit,
            "ON" => Token::On,
            "IS" => Token::Is,
            "NULL" => Token::Null,
            "TRUE" => Token::True,
            "FALSE" => Token::False,
            "EXISTS" => Token::Exists,
            "CASE" => Token::Case,
            "WHEN" => Token::When,
            "THEN" => Token::Then,
            "ELSE" => Token::Else,
            "END" => Token::End,
            "UNION" => Token::Union,
            "ALL" => Token::All,
            "CALL" => Token::Call,
            "LOAD" => Token::Load,
            "CSV" => Token::Csv,
            "FROM" => Token::From,
            _ => Token::Identifier(s),
        };

        Ok(token)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_match() {
        let tokens = Lexer::new("MATCH (n) RETURN n").tokenize().unwrap();
        assert_eq!(tokens[0], Token::Match);
        assert_eq!(tokens[1], Token::LParen);
        assert_eq!(tokens[2], Token::Identifier("n".into()));
        assert_eq!(tokens[3], Token::RParen);
        assert_eq!(tokens[4], Token::Return);
        assert_eq!(tokens[5], Token::Identifier("n".into()));
    }

    #[test]
    fn test_match_with_edge() {
        let tokens = Lexer::new("MATCH (a)-[:KNOWS]->(b) RETURN a, b")
            .tokenize()
            .unwrap();
        assert_eq!(tokens[0], Token::Match);
        assert_eq!(tokens[1], Token::LParen);
        assert_eq!(tokens[4], Token::Minus);
        assert_eq!(tokens[5], Token::LBracket);
        assert_eq!(tokens[6], Token::Colon);
        assert_eq!(tokens[7], Token::Identifier("KNOWS".into()));
    }

    #[test]
    fn test_string_literal() {
        let tokens = Lexer::new(r#"MATCH (n {name: "Alice"}) RETURN n"#)
            .tokenize()
            .unwrap();
        let string_tokens: Vec<_> = tokens
            .iter()
            .filter(|t| matches!(t, Token::StringLiteral(_)))
            .collect();
        assert_eq!(string_tokens.len(), 1);
        assert_eq!(string_tokens[0], &Token::StringLiteral("Alice".into()));
    }

    #[test]
    fn test_integer_and_float() {
        let tokens = Lexer::new("SET n.age = 30 SET n.score = 95.5")
            .tokenize()
            .unwrap();
        assert!(tokens.contains(&Token::IntegerLiteral(30)));
        assert!(tokens.contains(&Token::FloatLiteral(95.5)));
    }

    #[test]
    fn test_where_clause() {
        let tokens = Lexer::new("WHERE n.age > 30 AND n.active = true")
            .tokenize()
            .unwrap();
        assert_eq!(tokens[0], Token::Where);
        // WHERE(0) n(1) .(2) age(3) >(4) 30(5) AND(6) ...
        assert_eq!(tokens[4], Token::Gt);
        assert_eq!(tokens[6], Token::And);
    }

    #[test]
    fn test_comments() {
        let tokens = Lexer::new("MATCH (n) // comment\nRETURN n -- another")
            .tokenize()
            .unwrap();
        // Comments should be skipped
        assert_eq!(tokens[0], Token::Match);
        assert!(tokens
            .iter()
            .all(|t| !matches!(t, Token::StringLiteral(s) if s.contains("comment"))));
    }

    #[test]
    fn test_operators() {
        let tokens = Lexer::new("a >= b AND c <> d OR e =~ 'pattern'")
            .tokenize()
            .unwrap();
        assert!(tokens.contains(&Token::Ge));
        assert!(tokens.contains(&Token::NotEquals));
        assert!(tokens.contains(&Token::RegexMatch));
    }

    #[test]
    fn test_list_literal() {
        let tokens = Lexer::new("[1, 2, 3]").tokenize().unwrap();
        assert_eq!(tokens[0], Token::LBracket);
        assert_eq!(tokens[1], Token::IntegerLiteral(1));
        assert_eq!(tokens[2], Token::Comma);
    }

    #[test]
    fn test_variable_length_path() {
        let tokens = Lexer::new("MATCH (a)-[:KNOWS*1..5]->(b)")
            .tokenize()
            .unwrap();
        assert!(tokens.contains(&Token::Star));
        assert!(tokens.contains(&Token::IntegerLiteral(1)));
        assert!(tokens.contains(&Token::DoubleDot));
        assert!(tokens.contains(&Token::IntegerLiteral(5)));
    }

    #[test]
    fn test_unterminated_block_comment_is_error() {
        assert!(matches!(
            Lexer::new("MATCH (n) /* unfinished").tokenize(),
            Err(LexerError::UnterminatedComment(_))
        ));
    }

    #[test]
    fn test_plus_equals_token() {
        let tokens = Lexer::new("n += value").tokenize().unwrap();
        assert!(tokens.contains(&Token::FatArrow));
    }
}
