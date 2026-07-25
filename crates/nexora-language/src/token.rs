//! Token types for the Cypher lexer.

use serde::{Deserialize, Serialize};

/// A token produced by the lexer.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Token {
    // Keywords
    Match,
    Optional,
    Where,
    Create,
    Merge,
    Set,
    Remove,
    Delete,
    Detach,
    Return,
    With,
    Unwind,
    As,
    And,
    Or,
    Xor,
    Not,
    In,
    StartsWith,
    EndsWith,
    Contains,
    Distinct,
    Order,
    By,
    Asc,
    Desc,
    Ascending,
    Descending,
    Skip,
    Limit,
    On,
    Is,
    Null,
    True,
    False,
    Exists,
    Case,
    When,
    Then,
    Else,
    End,
    Union,
    All,
    Call,
    Load,
    Csv,
    From,

    // Symbols
    LParen,     // (
    RParen,     // )
    LBracket,   // [
    RBracket,   // ]
    LBrace,     // {
    RBrace,     // }
    Comma,      // ,
    Dot,        // .
    Colon,      // :
    Semicolon,  // ;
    Equals,     // =
    NotEquals,  // <>
    Lt,         // <
    Le,         // <=
    Gt,         // >
    Ge,         // >=
    Plus,       // +
    Minus,      // -
    Star,       // *
    Slash,      // /
    Percent,    // %
    Pipe,       // |
    ArrowRight, // ->
    ArrowLeft,  // <-
    Tilde,      // ~
    RegexMatch, // =~
    FatArrow,   // +=
    DoubleDot,  // ..

    // Literals
    IntegerLiteral(i64),
    FloatLiteral(f64),
    StringLiteral(String),

    // Identifiers
    Identifier(String),
    EscapedIdentifier(String),

    /// Label identifier (preceded by :)
    Label(String),

    /// End of input
    Eof,
}

impl Token {
    /// Is this token a keyword?
    pub fn is_keyword(&self) -> bool {
        matches!(
            self,
            Self::Match
                | Self::Optional
                | Self::Where
                | Self::Create
                | Self::Merge
                | Self::Set
                | Self::Remove
                | Self::Delete
                | Self::Detach
                | Self::Return
                | Self::With
                | Self::Unwind
                | Self::As
                | Self::And
                | Self::Or
                | Self::Xor
                | Self::Not
                | Self::In
                | Self::StartsWith
                | Self::EndsWith
                | Self::Contains
                | Self::Distinct
                | Self::Order
                | Self::By
                | Self::Asc
                | Self::Desc
                | Self::Ascending
                | Self::Descending
                | Self::Skip
                | Self::Limit
                | Self::On
                | Self::Is
                | Self::Null
                | Self::True
                | Self::False
                | Self::Exists
                | Self::Case
                | Self::When
                | Self::Then
                | Self::Else
                | Self::End
                | Self::Union
                | Self::All
                | Self::Call
                | Self::Load
                | Self::Csv
                | Self::From
        )
    }

    /// Get the string representation of a keyword token.
    pub fn keyword_str(&self) -> Option<&'static str> {
        match self {
            Self::Match => Some("MATCH"),
            Self::Optional => Some("OPTIONAL"),
            Self::Where => Some("WHERE"),
            Self::Create => Some("CREATE"),
            Self::Merge => Some("MERGE"),
            Self::Set => Some("SET"),
            Self::Remove => Some("REMOVE"),
            Self::Delete => Some("DELETE"),
            Self::Detach => Some("DETACH"),
            Self::Return => Some("RETURN"),
            Self::With => Some("WITH"),
            Self::Unwind => Some("UNWIND"),
            Self::As => Some("AS"),
            Self::And => Some("AND"),
            Self::Or => Some("OR"),
            Self::Xor => Some("XOR"),
            Self::Not => Some("NOT"),
            Self::In => Some("IN"),
            Self::StartsWith => Some("STARTS WITH"),
            Self::EndsWith => Some("ENDS WITH"),
            Self::Contains => Some("CONTAINS"),
            Self::Distinct => Some("DISTINCT"),
            Self::Order => Some("ORDER"),
            Self::By => Some("BY"),
            Self::Asc | Self::Ascending => Some("ASC"),
            Self::Desc | Self::Descending => Some("DESC"),
            Self::Skip => Some("SKIP"),
            Self::Limit => Some("LIMIT"),
            Self::On => Some("ON"),
            Self::Is => Some("IS"),
            Self::Null => Some("NULL"),
            Self::True => Some("TRUE"),
            Self::False => Some("FALSE"),
            Self::Exists => Some("EXISTS"),
            Self::Case => Some("CASE"),
            Self::When => Some("WHEN"),
            Self::Then => Some("THEN"),
            Self::Else => Some("ELSE"),
            Self::End => Some("END"),
            Self::Union => Some("UNION"),
            Self::All => Some("ALL"),
            Self::Call => Some("CALL"),
            Self::Load => Some("LOAD"),
            Self::Csv => Some("CSV"),
            Self::From => Some("FROM"),
            _ => None,
        }
    }
}

impl std::fmt::Display for Token {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::IntegerLiteral(n) => write!(f, "{n}"),
            Self::FloatLiteral(n) => write!(f, "{n}"),
            Self::StringLiteral(s) => write!(f, "\"{s}\""),
            Self::Identifier(s) => write!(f, "{s}"),
            Self::EscapedIdentifier(s) => write!(f, "`{s}`"),
            Self::Label(s) => write!(f, ":{s}"),
            Self::Eof => write!(f, "<EOF>"),
            _ => {
                if let Some(kw) = self.keyword_str() {
                    write!(f, "{kw}")
                } else {
                    write!(f, "{self:?}")
                }
            }
        }
    }
}
