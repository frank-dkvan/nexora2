//! Cypher recursive descent parser.
//!
//! Parses a token stream into a CypherQuery AST.
//! Supports: MATCH, CREATE, MERGE, SET, DELETE, WITH, RETURN, WHERE, UNWIND.

use crate::ast::*;
use crate::lexer::Lexer;
use crate::token::Token;
use nexora_id::PropertyValue;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ParseError {
    #[error("unexpected token: expected {expected}, found {found} at position {pos}")]
    UnexpectedToken {
        expected: String,
        found: String,
        pos: usize,
    },
    #[error("unexpected end of input")]
    UnexpectedEof,
    #[error("lexer error: {0}")]
    LexerError(#[from] crate::lexer::LexerError),
    #[error("expression nesting too deep (max {max_depth})")]
    NestingTooDeep { max_depth: usize },
}

/// Cypher parser.
pub struct Parser {
    tokens: Vec<Token>,
    pos: usize,
    /// Recursion depth counter — prevents stack overflow on deeply nested
    /// expressions (e.g. 5 000 parens or NOT NOT NOT … chains).
    depth: usize,
}

/// Maximum allowed recursion depth. A typical stack-overflow payload is
/// ~5 000 levels; we reject anything deeper than 256, which is already
/// far beyond any legitimate query.
const MAX_DEPTH: usize = 256;

impl Parser {
    /// Parse a Cypher query string.
    pub fn parse(input: &str) -> Result<CypherQuery, ParseError> {
        let tokens = Lexer::new(input).tokenize()?;
        let mut parser = Self {
            tokens,
            pos: 0,
            depth: 0,
        };
        let query = parser.parse_query()?;
        Ok(query)
    }

    fn peek(&self) -> &Token {
        self.tokens.get(self.pos).unwrap_or(&Token::Eof)
    }

    fn advance(&mut self) -> Token {
        let tok = self.tokens.get(self.pos).cloned().unwrap_or(Token::Eof);
        self.pos += 1;
        tok
    }

    fn expect(&mut self, expected: &Token) -> Result<Token, ParseError> {
        let tok = self.advance();
        if &tok == expected {
            Ok(tok)
        } else {
            Err(ParseError::UnexpectedToken {
                expected: format!("{expected:?}"),
                found: format!("{tok:?}"),
                pos: self.pos,
            })
        }
    }

    fn expect_identifier(&mut self) -> Result<String, ParseError> {
        match self.advance() {
            Token::Identifier(s) => Ok(s),
            Token::EscapedIdentifier(s) => Ok(s),
            tok => Err(ParseError::UnexpectedToken {
                expected: "identifier".into(),
                found: format!("{tok:?}"),
                pos: self.pos,
            }),
        }
    }

    fn at_end(&self) -> bool {
        matches!(self.peek(), Token::Eof)
    }

    // ============================================================
    // Query parsing
    // ============================================================

    fn parse_query(&mut self) -> Result<CypherQuery, ParseError> {
        let first_subquery = self.parse_subquery()?;

        // Check for UNION
        if *self.peek() == Token::Union {
            let mut queries = vec![first_subquery];
            let mut all = false;
            while *self.peek() == Token::Union {
                self.advance(); // consume UNION
                all = if *self.peek() == Token::All {
                    self.advance();
                    true
                } else {
                    false
                };
                queries.push(self.parse_subquery()?);
            }
            return Ok(CypherQuery {
                clauses: vec![Clause::Union { all, queries }],
                as_of: None,
            });
        }

        // No UNION — check for trailing semicolon
        if *self.peek() == Token::Semicolon {
            self.advance();
            if !self.at_end() {
                return Err(ParseError::UnexpectedToken {
                    expected: "end of input after semicolon".into(),
                    found: format!("{:?}", self.peek()),
                    pos: self.pos,
                });
            }
        }

        Ok(CypherQuery {
            clauses: first_subquery,
            as_of: None,
        })
    }

    /// Parse a sub-query: a sequence of clauses terminated by UNION, semicolon,
    /// closing brace (for CALL subqueries), or EOF.
    fn parse_subquery(&mut self) -> Result<Vec<Clause>, ParseError> {
        let mut clauses = Vec::new();
        while !self.at_end()
            && *self.peek() != Token::Union
            && *self.peek() != Token::Semicolon
            && *self.peek() != Token::RBrace
        {
            clauses.push(self.parse_clause()?);
        }
        Ok(clauses)
    }

    fn parse_clause(&mut self) -> Result<Clause, ParseError> {
        match self.peek().clone() {
            Token::Match | Token::Optional => self.parse_match(),
            Token::Create => self.parse_create(),
            Token::Merge => self.parse_merge(),
            Token::Set => self.parse_set(),
            Token::Remove => self.parse_remove(),
            Token::Delete | Token::Detach => self.parse_delete(),
            Token::Return => self.parse_return(),
            Token::With => self.parse_with(),
            Token::Unwind => self.parse_unwind(),
            Token::Call => self.parse_call(),
            Token::Load => self.parse_load_csv(),
            tok => Err(ParseError::UnexpectedToken {
                expected: "clause keyword (MATCH, CREATE, MERGE, SET, RETURN, etc.)".into(),
                found: format!("{tok:?}"),
                pos: self.pos,
            }),
        }
    }

    // ============================================================
    // MATCH clause
    // ============================================================

    fn parse_match(&mut self) -> Result<Clause, ParseError> {
        let optional = if *self.peek() == Token::Optional {
            self.advance();
            self.expect(&Token::Match)?;
            true
        } else {
            self.advance(); // consume MATCH
            false
        };

        let pattern = self.parse_pattern()?;
        let predicate = if *self.peek() == Token::Where {
            self.advance();
            Some(self.parse_expression()?)
        } else {
            None
        };

        Ok(Clause::Match {
            optional,
            pattern,
            predicate,
        })
    }

    // ============================================================
    // Pattern parsing
    // ============================================================

    fn parse_pattern(&mut self) -> Result<Pattern, ParseError> {
        let mut parts = Vec::new();
        parts.push(self.parse_pattern_part()?);
        while *self.peek() == Token::Comma {
            self.advance();
            parts.push(self.parse_pattern_part()?);
        }
        Ok(Pattern { parts })
    }

    fn parse_pattern_part(&mut self) -> Result<PatternPart, ParseError> {
        // Check for path variable: var = (...)
        let variable = if matches!(self.peek(), Token::Identifier(_)) {
            // Look ahead for '='
            if self.pos + 1 < self.tokens.len() && self.tokens[self.pos + 1] == Token::Equals {
                let var = self.expect_identifier()?;
                self.expect(&Token::Equals)?; // consume '=' properly
                Some(var)
            } else {
                None
            }
        } else {
            None
        };

        let chain = self.parse_pattern_chain()?;
        Ok(PatternPart { variable, chain })
    }

    fn parse_pattern_chain(&mut self) -> Result<PatternChain, ParseError> {
        let mut segments = Vec::new();

        // First node
        let node = self.parse_node_pattern()?;
        segments.push(PatternSegment { node, edge: None });

        // Edge + Node pairs: edge belongs to the source segment
        while matches!(self.peek(), Token::Minus | Token::ArrowLeft) {
            let edge = self.parse_edge_pattern()?;
            let next_node = self.parse_node_pattern()?;

            // Attach edge to the LAST segment (source node)
            let last = segments.last_mut().unwrap();
            last.edge = Some(edge);

            // Push the target node as a new segment
            segments.push(PatternSegment {
                node: next_node,
                edge: None,
            });
        }

        Ok(PatternChain { segments })
    }

    fn parse_node_pattern(&mut self) -> Result<NodePattern, ParseError> {
        self.expect(&Token::LParen)?;

        let mut variable = None;
        let mut labels = Vec::new();
        let mut properties = Vec::new();

        // Variable
        if matches!(self.peek(), Token::Identifier(_)) {
            variable = Some(self.expect_identifier()?);
        }

        // Labels
        while *self.peek() == Token::Colon {
            self.advance();
            labels.push(self.expect_identifier()?);
        }

        // Properties {key: val, ...}
        if *self.peek() == Token::LBrace {
            properties = self.parse_map_literal()?;
        }

        self.expect(&Token::RParen)?;

        Ok(NodePattern {
            variable,
            labels,
            properties,
        })
    }

    fn parse_edge_pattern(&mut self) -> Result<EdgePattern, ParseError> {
        // Determine direction
        let mut direction = if *self.peek() == Token::ArrowLeft {
            self.advance(); // consume <-
            EdgeDirection::Incoming
        } else {
            self.advance(); // consume -
            EdgeDirection::Outgoing
        };

        // Check for edge details: [var:TYPE*min..max]
        let mut variable = None;
        let mut edge_type = None;
        let mut properties = Vec::new();
        let mut min_hops = None;
        let mut max_hops = None;

        if *self.peek() == Token::LBracket {
            self.advance();

            // Variable
            if matches!(self.peek(), Token::Identifier(_)) {
                variable = Some(self.expect_identifier()?);
            }

            // Edge type
            if *self.peek() == Token::Colon {
                self.advance();
                edge_type = Some(self.expect_identifier()?);
            }

            // Variable-length path *min..max
            if *self.peek() == Token::Star {
                self.advance();
                if matches!(self.peek(), Token::IntegerLiteral(_)) {
                    if let Token::IntegerLiteral(min) = self.advance() {
                        min_hops = Some(min as u32);
                    }
                    if *self.peek() == Token::DoubleDot {
                        self.advance();
                        if matches!(self.peek(), Token::IntegerLiteral(_)) {
                            if let Token::IntegerLiteral(max) = self.advance() {
                                max_hops = Some(max as u32);
                            }
                        }
                    } else {
                        // *N means exactly N hops
                        max_hops = min_hops;
                    }
                } else {
                    // * means 1..unbounded
                    min_hops = Some(1);
                    if *self.peek() == Token::DoubleDot {
                        self.advance();
                        if let Token::IntegerLiteral(max) = self.advance() {
                            max_hops = Some(max as u32);
                        } else {
                            return Err(ParseError::UnexpectedToken {
                                expected: "maximum path length".into(),
                                found: format!("{:?}", self.peek()),
                                pos: self.pos,
                            });
                        }
                    }
                }
            }

            // Properties
            if *self.peek() == Token::LBrace {
                properties = self.parse_map_literal()?;
            }

            self.expect(&Token::RBracket)?;
        }

        // Closing direction
        if *self.peek() == Token::ArrowRight {
            self.advance();
            if direction == EdgeDirection::Incoming {
                // `<-[...]->` is not standard, but we accept it
            }
        } else if *self.peek() == Token::Minus {
            self.advance();
            if direction == EdgeDirection::Outgoing {
                direction = EdgeDirection::Either;
            }
        }

        Ok(EdgePattern {
            variable,
            edge_type,
            properties,
            direction,
            min_hops,
            max_hops,
        })
    }

    // ============================================================
    // Expression parsing (operator precedence climbing)
    // ============================================================

    fn parse_expression(&mut self) -> Result<Expression, ParseError> {
        self.inc_depth()?;
        let r = self.parse_or();
        self.dec_depth();
        r
    }

    fn inc_depth(&mut self) -> Result<(), ParseError> {
        self.depth += 1;
        if self.depth > MAX_DEPTH {
            return Err(ParseError::NestingTooDeep {
                max_depth: MAX_DEPTH,
            });
        }
        Ok(())
    }

    fn dec_depth(&mut self) {
        self.depth = self.depth.saturating_sub(1);
    }

    fn parse_or(&mut self) -> Result<Expression, ParseError> {
        let mut left = self.parse_xor()?;
        while *self.peek() == Token::Or {
            self.advance();
            let right = self.parse_xor()?;
            left = Expression::BinOp {
                op: BinaryOp::Or,
                left: Box::new(left),
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    fn parse_xor(&mut self) -> Result<Expression, ParseError> {
        let mut left = self.parse_and()?;
        while *self.peek() == Token::Xor {
            self.advance();
            let right = self.parse_and()?;
            left = Expression::BinOp {
                op: BinaryOp::Xor,
                left: Box::new(left),
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    fn parse_and(&mut self) -> Result<Expression, ParseError> {
        let mut left = self.parse_not()?;
        while *self.peek() == Token::And {
            self.advance();
            let right = self.parse_not()?;
            left = Expression::BinOp {
                op: BinaryOp::And,
                left: Box::new(left),
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    fn parse_not(&mut self) -> Result<Expression, ParseError> {
        if *self.peek() == Token::Not {
            self.advance();
            let operand = self.parse_not()?;
            Ok(Expression::UnaryOp {
                op: UnaryOp::Not,
                operand: Box::new(operand),
            })
        } else {
            self.parse_comparison()
        }
    }

    fn parse_comparison(&mut self) -> Result<Expression, ParseError> {
        let mut left = self.parse_additive()?;

        let op = match self.peek() {
            Token::Equals => Some(BinaryOp::Eq),
            Token::NotEquals => Some(BinaryOp::Ne),
            Token::Lt => Some(BinaryOp::Lt),
            Token::Le => Some(BinaryOp::Le),
            Token::Gt => Some(BinaryOp::Gt),
            Token::Ge => Some(BinaryOp::Ge),
            Token::StartsWith => Some(BinaryOp::StartsWith),
            Token::EndsWith => Some(BinaryOp::EndsWith),
            Token::Contains => Some(BinaryOp::Contains),
            Token::RegexMatch => Some(BinaryOp::RegexMatch),
            _ => None,
        };

        if let Some(op) = op {
            self.advance();
            if matches!(op, BinaryOp::StartsWith | BinaryOp::EndsWith)
                && *self.peek() == Token::With
            {
                self.advance();
            }
            let right = self.parse_additive()?;
            left = Expression::BinOp {
                op,
                left: Box::new(left),
                right: Box::new(right),
            };
        }

        // IS NULL / IS NOT NULL
        if *self.peek() == Token::Is {
            self.advance();
            if *self.peek() == Token::Not {
                self.advance();
                self.expect(&Token::Null)?;
                left = Expression::IsNotNull(Box::new(left));
            } else {
                self.expect(&Token::Null)?;
                left = Expression::IsNull(Box::new(left));
            }
        }

        Ok(left)
    }

    fn parse_additive(&mut self) -> Result<Expression, ParseError> {
        let mut left = self.parse_multiplicative()?;
        loop {
            let op = match self.peek() {
                Token::Plus => BinaryOp::Add,
                Token::Minus => BinaryOp::Sub,
                _ => break,
            };
            self.advance();
            let right = self.parse_multiplicative()?;
            left = Expression::BinOp {
                op,
                left: Box::new(left),
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    fn parse_multiplicative(&mut self) -> Result<Expression, ParseError> {
        let mut left = self.parse_unary()?;
        loop {
            let op = match self.peek() {
                Token::Star => BinaryOp::Mul,
                Token::Slash => BinaryOp::Div,
                Token::Percent => BinaryOp::Mod,
                _ => break,
            };
            self.advance();
            let right = self.parse_unary()?;
            left = Expression::BinOp {
                op,
                left: Box::new(left),
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    fn parse_unary(&mut self) -> Result<Expression, ParseError> {
        match self.peek() {
            Token::Minus => {
                self.advance();
                let operand = self.parse_unary()?;
                Ok(Expression::UnaryOp {
                    op: UnaryOp::Neg,
                    operand: Box::new(operand),
                })
            }
            _ => self.parse_postfix(),
        }
    }

    fn parse_postfix(&mut self) -> Result<Expression, ParseError> {
        let mut expr = self.parse_primary()?;

        loop {
            match self.peek() {
                // Property access: expr.prop
                Token::Dot => {
                    self.advance();
                    let prop = self.expect_identifier()?;
                    expr = Expression::Property(Box::new(expr), prop);
                }
                // IN list: expr IN expr
                Token::In => {
                    self.advance();
                    let list = self.parse_primary()?;
                    expr = Expression::BinOp {
                        op: BinaryOp::In,
                        left: Box::new(expr),
                        right: Box::new(list),
                    };
                }
                _ => break,
            }
        }

        Ok(expr)
    }

    fn parse_primary(&mut self) -> Result<Expression, ParseError> {
        match self.peek().clone() {
            // Integer literal
            Token::IntegerLiteral(n) => {
                self.advance();
                Ok(Expression::Literal(PropertyValue::Integer(n)))
            }
            // Float literal
            Token::FloatLiteral(n) => {
                self.advance();
                Ok(Expression::Literal(PropertyValue::Float(n)))
            }
            // String literal
            Token::StringLiteral(s) => {
                self.advance();
                Ok(Expression::Literal(PropertyValue::String(s)))
            }
            // Boolean literals
            Token::True => {
                self.advance();
                Ok(Expression::Literal(PropertyValue::Boolean(true)))
            }
            Token::False => {
                self.advance();
                Ok(Expression::Literal(PropertyValue::Boolean(false)))
            }
            // Null
            Token::Null => {
                self.advance();
                Ok(Expression::Literal(PropertyValue::null()))
            }
            // CASE expression
            Token::Case => self.parse_case(),
            // EXISTS subquery
            Token::Exists => self.parse_exists(),
            // Parenthesized expression
            Token::LParen => {
                self.advance();
                let expr = self.parse_expression()?;
                self.expect(&Token::RParen)?;
                Ok(Expression::Parenthesized(Box::new(expr)))
            }
            // List literal or List Comprehension
            Token::LBracket => {
                self.advance();
                // Check for list comprehension: [var IN expr ...]
                // Need lookahead: identifier followed by IN
                let is_comprehension = matches!(self.peek(), Token::Identifier(_))
                    && self.pos + 1 < self.tokens.len()
                    && matches!(self.tokens[self.pos + 1], Token::In);

                if is_comprehension {
                    let variable = self.expect_identifier()?;
                    self.advance(); // consume IN
                    let list = self.parse_expression()?;

                    let predicate = if *self.peek() == Token::Where {
                        self.advance();
                        Some(Box::new(self.parse_expression()?))
                    } else {
                        None
                    };

                    let projection = if *self.peek() == Token::Pipe {
                        self.advance();
                        Some(Box::new(self.parse_expression()?))
                    } else {
                        None
                    };

                    self.expect(&Token::RBracket)?;
                    Ok(Expression::ListComprehension {
                        variable,
                        list: Box::new(list),
                        predicate,
                        projection,
                    })
                } else {
                    // Plain list literal
                    let mut items = Vec::new();
                    while *self.peek() != Token::RBracket {
                        items.push(self.parse_expression()?);
                        if *self.peek() == Token::Comma {
                            self.advance();
                        }
                    }
                    self.expect(&Token::RBracket)?;
                    Ok(Expression::List(items))
                }
            }
            Token::LBrace => Ok(Expression::Map(self.parse_map_literal()?)),
            // Identifier (variable or function call)
            Token::Identifier(name) => {
                self.advance();
                // Check for function call: name(...)
                if *self.peek() == Token::LParen {
                    self.advance();
                    let distinct = if *self.peek() == Token::Distinct {
                        self.advance();
                        true
                    } else {
                        false
                    };
                    let mut args = Vec::new();
                    while *self.peek() != Token::RParen {
                        args.push(self.parse_expression()?);
                        if *self.peek() == Token::Comma {
                            self.advance();
                        }
                    }
                    self.expect(&Token::RParen)?;

                    // Check for aggregation functions
                    let agg = match name.to_lowercase().as_str() {
                        "count" => Some(AggFunction::Count),
                        "sum" => Some(AggFunction::Sum),
                        "avg" => Some(AggFunction::Avg),
                        "min" => Some(AggFunction::Min),
                        "max" => Some(AggFunction::Max),
                        "collect" => Some(AggFunction::Collect),
                        "percentilecont" => Some(AggFunction::PercentileCont),
                        "percentiledisc" => Some(AggFunction::PercentileDisc),
                        "stdev" => Some(AggFunction::StDev),
                        "stdevp" => Some(AggFunction::StDevP),
                        _ => None,
                    };

                    if let Some(func) = agg {
                        let expr = args
                            .into_iter()
                            .next()
                            .unwrap_or(Expression::Literal(PropertyValue::null()));
                        Ok(Expression::Aggregation {
                            function: func,
                            expr: Box::new(expr),
                            distinct,
                        })
                    } else {
                        Ok(Expression::Function {
                            name,
                            args,
                            distinct,
                        })
                    }
                } else {
                    Ok(Expression::Variable(name))
                }
            }
            // Asterisk (for count(*))
            Token::Star => {
                self.advance();
                Ok(Expression::Variable("*".into()))
            }
            tok => Err(ParseError::UnexpectedToken {
                expected: "expression".into(),
                found: format!("{tok:?}"),
                pos: self.pos,
            }),
        }
    }

    // ============================================================
    // CASE expression
    // ============================================================

    /// Parse a CASE expression:
    ///   CASE [expr] WHEN val THEN result [WHEN ...] [ELSE result] END
    fn parse_case(&mut self) -> Result<Expression, ParseError> {
        self.advance(); // consume CASE

        // Optional case expression: CASE n.age WHEN 18 THEN "adult" ... END
        let expr = if !matches!(self.peek(), Token::When) {
            Some(Box::new(self.parse_expression()?))
        } else {
            None
        };

        let mut whens = Vec::new();
        while *self.peek() == Token::When {
            self.advance(); // consume WHEN
            let when_expr = self.parse_expression()?;
            self.expect(&Token::Then)?; // consume THEN
            let then_expr = self.parse_expression()?;
            whens.push((when_expr, then_expr));
        }

        let else_expr = if *self.peek() == Token::Else {
            self.advance(); // consume ELSE
            Some(Box::new(self.parse_expression()?))
        } else {
            None
        };

        self.expect(&Token::End)?; // consume END

        Ok(Expression::Case {
            expr,
            whens,
            else_expr,
        })
    }

    /// Parse EXISTS { MATCH pattern [WHERE ...] }
    fn parse_exists(&mut self) -> Result<Expression, ParseError> {
        self.advance(); // consume EXISTS
        self.expect(&Token::LBrace)?; // consume {
        self.expect(&Token::Match)?; // consume MATCH
        let pattern = self.parse_pattern()?;
        // Optional WHERE
        let _predicate = if *self.peek() == Token::Where {
            self.advance();
            Some(self.parse_expression()?)
        } else {
            None
        };
        self.expect(&Token::RBrace)?; // consume }

        Ok(Expression::ExistsPattern(Box::new(pattern)))
    }

    // ============================================================
    // Map literal {key: val, ...}
    // ============================================================

    fn parse_map_literal(&mut self) -> Result<Vec<(String, Expression)>, ParseError> {
        self.expect(&Token::LBrace)?;
        let mut entries = Vec::new();
        while *self.peek() != Token::RBrace {
            let key = self.expect_identifier()?;
            self.expect(&Token::Colon)?;
            let value = self.parse_expression()?;
            entries.push((key, value));
            if *self.peek() == Token::Comma {
                self.advance();
            }
        }
        self.expect(&Token::RBrace)?;
        Ok(entries)
    }

    // ============================================================
    // CREATE clause
    // ============================================================

    fn parse_create(&mut self) -> Result<Clause, ParseError> {
        self.advance(); // consume CREATE
        let pattern = self.parse_pattern()?;
        Ok(Clause::Create { pattern })
    }

    // ============================================================
    // MERGE clause
    // ============================================================

    fn parse_merge(&mut self) -> Result<Clause, ParseError> {
        self.advance(); // consume MERGE
        let pattern = self.parse_pattern()?;

        let mut on_create = Vec::new();
        let mut on_match = Vec::new();

        while matches!(self.peek(), Token::On) {
            self.advance(); // consume ON
            match self.peek() {
                Token::Create => {
                    self.advance(); // consume CREATE
                    self.expect(&Token::Set)?; // consume SET
                    on_create.push(self.parse_set_item()?);
                    while *self.peek() == Token::Comma {
                        self.advance();
                        on_create.push(self.parse_set_item()?);
                    }
                }
                Token::Match => {
                    self.advance(); // consume MATCH
                    self.expect(&Token::Set)?; // consume SET
                    on_match.push(self.parse_set_item()?);
                    while *self.peek() == Token::Comma {
                        self.advance();
                        on_match.push(self.parse_set_item()?);
                    }
                }
                tok => {
                    return Err(ParseError::UnexpectedToken {
                        expected: "CREATE or MATCH after ON".into(),
                        found: format!("{tok:?}"),
                        pos: self.pos,
                    });
                }
            }
        }

        Ok(Clause::Merge {
            pattern,
            on_create,
            on_match,
        })
    }

    // ============================================================
    // SET clause
    // ============================================================

    fn parse_set(&mut self) -> Result<Clause, ParseError> {
        self.advance(); // consume SET
        let mut items = Vec::new();
        items.push(self.parse_set_item()?);
        while *self.peek() == Token::Comma {
            self.advance();
            items.push(self.parse_set_item()?);
        }
        Ok(Clause::Set { items })
    }

    fn parse_set_item(&mut self) -> Result<SetItem, ParseError> {
        // Parse target WITHOUT comparison operators (to avoid consuming the '=' in SET x = val)
        let target = self.parse_set_target()?;
        if *self.peek() == Token::Equals {
            self.advance();
            let value = self.parse_expression()?;
            Ok(SetItem::Property { target, value })
        } else if *self.peek() == Token::FatArrow {
            self.advance();
            let map = self.parse_expression()?;
            Ok(SetItem::MapProjection { target, map })
        } else if *self.peek() == Token::Colon {
            self.advance();
            let label = self.expect_identifier()?;
            Ok(SetItem::Label { target, label })
        } else {
            Err(ParseError::UnexpectedToken {
                expected: "= or += or :".into(),
                found: format!("{:?}", self.peek()),
                pos: self.pos,
            })
        }
    }

    /// Parse a SET target expression (no comparison operators).
    /// Parses: variable, property access, function calls.
    /// Does NOT parse: =, <>, <, >, AND, OR, etc.
    fn parse_set_target(&mut self) -> Result<Expression, ParseError> {
        let mut expr = self.parse_primary()?;
        while matches!(self.peek(), Token::Dot) {
            self.advance();
            let prop = self.expect_identifier()?;
            expr = Expression::Property(Box::new(expr), prop);
        }
        Ok(expr)
    }

    // ============================================================
    // REMOVE clause
    // ============================================================

    fn parse_remove(&mut self) -> Result<Clause, ParseError> {
        self.advance(); // consume REMOVE
        let mut items = Vec::new();
        items.push(self.parse_remove_item()?);
        while *self.peek() == Token::Comma {
            self.advance();
            items.push(self.parse_remove_item()?);
        }
        Ok(Clause::Remove { items })
    }

    fn parse_remove_item(&mut self) -> Result<RemoveItem, ParseError> {
        let target = Expression::Variable(self.expect_identifier()?);
        if *self.peek() == Token::Dot {
            self.advance();
            let key = self.expect_identifier()?;
            Ok(RemoveItem::Property { target, key })
        } else if *self.peek() == Token::Colon {
            self.advance();
            let label = self.expect_identifier()?;
            Ok(RemoveItem::Label { target, label })
        } else {
            Err(ParseError::UnexpectedToken {
                expected: ". or :".into(),
                found: format!("{:?}", self.peek()),
                pos: self.pos,
            })
        }
    }

    // ============================================================
    // DELETE clause
    // ============================================================

    fn parse_delete(&mut self) -> Result<Clause, ParseError> {
        let detach = if *self.peek() == Token::Detach {
            self.advance(); // consume DETACH
            true
        } else {
            false
        };
        self.advance(); // consume DELETE

        let mut expressions = Vec::new();
        expressions.push(self.parse_expression()?);
        while *self.peek() == Token::Comma {
            self.advance();
            expressions.push(self.parse_expression()?);
        }

        Ok(Clause::Delete {
            expressions,
            detach,
        })
    }

    // ============================================================
    // RETURN clause
    // ============================================================

    fn parse_return(&mut self) -> Result<Clause, ParseError> {
        self.advance(); // consume RETURN
        let distinct = if *self.peek() == Token::Distinct {
            self.advance();
            true
        } else {
            false
        };

        let items = self.parse_return_items()?;
        let order_by = self.parse_order_by()?;
        let skip = self.parse_skip()?;
        let limit = self.parse_limit()?;

        Ok(Clause::Return {
            items,
            order_by,
            skip,
            limit,
            distinct,
        })
    }

    fn parse_return_items(&mut self) -> Result<Vec<ReturnItem>, ParseError> {
        let mut items = Vec::new();
        items.push(self.parse_return_item()?);
        while *self.peek() == Token::Comma {
            self.advance();
            items.push(self.parse_return_item()?);
        }
        Ok(items)
    }

    fn parse_return_item(&mut self) -> Result<ReturnItem, ParseError> {
        let expression = self.parse_expression()?;
        let alias = if *self.peek() == Token::As {
            self.advance();
            Some(self.expect_identifier()?)
        } else {
            None
        };
        Ok(ReturnItem { expression, alias })
    }

    // ============================================================
    // WITH clause (similar to RETURN but mid-query)
    // ============================================================

    fn parse_with(&mut self) -> Result<Clause, ParseError> {
        self.advance(); // consume WITH
        let distinct = if *self.peek() == Token::Distinct {
            self.advance();
            true
        } else {
            false
        };

        let items = self.parse_return_items()?;
        let where_clause = if *self.peek() == Token::Where {
            self.advance();
            Some(self.parse_expression()?)
        } else {
            None
        };
        let order_by = self.parse_order_by()?;
        let skip = self.parse_skip()?;
        let limit = self.parse_limit()?;

        Ok(Clause::With {
            items,
            where_clause,
            order_by,
            skip,
            limit,
            distinct,
        })
    }

    // ============================================================
    // UNWIND clause
    // ============================================================

    fn parse_unwind(&mut self) -> Result<Clause, ParseError> {
        self.advance(); // consume UNWIND
        let expression = self.parse_expression()?;
        self.expect(&Token::As)?;
        let alias = self.expect_identifier()?;
        Ok(Clause::Unwind { expression, alias })
    }

    // ============================================================
    // CALL { subquery }
    // ============================================================

    fn parse_call(&mut self) -> Result<Clause, ParseError> {
        self.advance(); // consume CALL
        self.expect(&Token::LBrace)?; // consume {
        let subquery = self.parse_subquery()?;
        self.expect(&Token::RBrace)?; // consume }
        Ok(Clause::Call { subquery })
    }

    // ============================================================
    // LOAD CSV clause
    // ============================================================

    fn parse_load_csv(&mut self) -> Result<Clause, ParseError> {
        self.advance(); // consume LOAD
        self.expect(&Token::Csv)?; // consume CSV

        // Optional WITH HEADERS
        let with_headers = if *self.peek() == Token::With {
            self.advance(); // consume WITH
            let id = self.expect_identifier()?;
            if id.eq_ignore_ascii_case("HEADERS") {
                true
            } else {
                return Err(ParseError::UnexpectedToken {
                    expected: "HEADERS after WITH in LOAD CSV".into(),
                    found: id,
                    pos: self.pos,
                });
            }
        } else {
            false
        };

        self.expect(&Token::From)?; // consume FROM

        // Path: string literal
        let path = match self.advance() {
            Token::StringLiteral(s) => s,
            tok => {
                return Err(ParseError::UnexpectedToken {
                    expected: "string literal (CSV path)".into(),
                    found: format!("{tok:?}"),
                    pos: self.pos,
                });
            }
        };

        self.expect(&Token::As)?; // consume AS
        let alias = self.expect_identifier()?;

        Ok(Clause::LoadCsv {
            path,
            alias,
            with_headers,
        })
    }

    // ============================================================
    // ORDER BY, SKIP, LIMIT
    // ============================================================

    fn parse_order_by(&mut self) -> Result<Option<OrderBy>, ParseError> {
        if *self.peek() == Token::Order {
            self.advance();
            self.expect(&Token::By)?;
            let mut items = Vec::new();
            items.push(self.parse_sort_item()?);
            while *self.peek() == Token::Comma {
                self.advance();
                items.push(self.parse_sort_item()?);
            }
            Ok(Some(OrderBy { items }))
        } else {
            Ok(None)
        }
    }

    fn parse_sort_item(&mut self) -> Result<SortItem, ParseError> {
        let expression = self.parse_expression()?;
        let ascending = match self.peek() {
            Token::Ascending => {
                self.advance();
                true
            }
            Token::Descending => {
                self.advance();
                false
            }
            _ => true,
        };
        Ok(SortItem {
            expression,
            ascending,
        })
    }

    fn parse_skip(&mut self) -> Result<Option<Expression>, ParseError> {
        if *self.peek() == Token::Skip {
            self.advance();
            Ok(Some(self.parse_expression()?))
        } else {
            Ok(None)
        }
    }

    fn parse_limit(&mut self) -> Result<Option<Expression>, ParseError> {
        if *self.peek() == Token::Limit {
            self.advance();
            Ok(Some(self.parse_expression()?))
        } else {
            Ok(None)
        }
    }
}

// ============================================================
// Tests
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_match_return() {
        let q = Parser::parse("MATCH (n) RETURN n").unwrap();
        assert_eq!(q.clauses.len(), 2);
        assert!(matches!(&q.clauses[0], Clause::Match { .. }));
        assert!(matches!(&q.clauses[1], Clause::Return { .. }));
    }

    #[test]
    fn test_parse_match_with_label() {
        let q = Parser::parse("MATCH (p:Person) RETURN p").unwrap();
        if let Clause::Match { pattern, .. } = &q.clauses[0] {
            let node = &pattern.parts[0].chain.segments[0].node;
            assert_eq!(node.labels, vec!["Person"]);
        } else {
            panic!("Expected Match clause");
        }
    }

    #[test]
    fn test_parse_match_with_edge() {
        let q = Parser::parse("MATCH (a)-[:KNOWS]->(b) RETURN a, b").unwrap();
        if let Clause::Match { pattern, .. } = &q.clauses[0] {
            let chain = &pattern.parts[0].chain;
            assert_eq!(chain.segments.len(), 2);
            let edge = chain.segments[0].edge.as_ref().unwrap();
            assert_eq!(edge.edge_type.as_deref(), Some("KNOWS"));
            assert_eq!(edge.direction, EdgeDirection::Outgoing);
        } else {
            panic!("Expected Match clause");
        }
    }

    #[test]
    fn test_parse_where_clause() {
        let q = Parser::parse("MATCH (n) WHERE n.age > 30 RETURN n").unwrap();
        if let Clause::Match { predicate, .. } = &q.clauses[0] {
            assert!(predicate.is_some());
            let pred = predicate.as_ref().unwrap();
            assert!(matches!(
                pred,
                Expression::BinOp {
                    op: BinaryOp::Gt,
                    ..
                }
            ));
        } else {
            panic!("Expected Match clause");
        }
    }

    #[test]
    fn test_parse_create_node() {
        let q = Parser::parse("CREATE (n:Person {name: \"Alice\", age: 30})").unwrap();
        if let Clause::Create { pattern } = &q.clauses[0] {
            let node = &pattern.parts[0].chain.segments[0].node;
            assert_eq!(node.labels, vec!["Person"]);
            assert_eq!(node.properties.len(), 2);
        } else {
            panic!("Expected Create clause");
        }
    }

    #[test]
    fn test_parse_set_property() {
        let q = Parser::parse("MATCH (n) SET n.age = 31 RETURN n").unwrap();
        assert!(matches!(&q.clauses[1], Clause::Set { .. }));
    }

    #[test]
    fn test_parse_delete() {
        let q = Parser::parse("MATCH (n) DELETE n").unwrap();
        assert!(matches!(
            &q.clauses[1],
            Clause::Delete { detach: false, .. }
        ));
    }

    #[test]
    fn test_parse_detach_delete() {
        let q = Parser::parse("MATCH (n) DETACH DELETE n").unwrap();
        assert!(matches!(&q.clauses[1], Clause::Delete { detach: true, .. }));
    }

    #[test]
    fn test_parse_return_with_limit() {
        let q = Parser::parse("MATCH (n) RETURN n LIMIT 10").unwrap();
        if let Clause::Return { limit, .. } = &q.clauses[1] {
            assert!(limit.is_some());
            assert_eq!(
                limit.as_ref().unwrap(),
                &Expression::Literal(PropertyValue::Integer(10))
            );
        } else {
            panic!("Expected Return clause");
        }
    }

    #[test]
    fn test_parse_return_with_order_by() {
        let q = Parser::parse("MATCH (n) RETURN n.name ORDER BY n.age DESC").unwrap();
        if let Clause::Return { order_by, .. } = &q.clauses[1] {
            assert!(order_by.is_some());
            let ob = order_by.as_ref().unwrap();
            assert_eq!(ob.items.len(), 1);
            assert!(!ob.items[0].ascending);
        } else {
            panic!("Expected Return clause");
        }
    }

    #[test]
    fn test_parse_count_aggregation() {
        let q = Parser::parse("MATCH (n:Person) RETURN count(*) AS total").unwrap();
        if let Clause::Return { items, .. } = &q.clauses[1] {
            assert_eq!(items.len(), 1);
            assert_eq!(items[0].alias.as_deref(), Some("total"));
            assert!(matches!(
                &items[0].expression,
                Expression::Aggregation {
                    function: AggFunction::Count,
                    ..
                }
            ));
        } else {
            panic!("Expected Return clause");
        }
    }

    #[test]
    fn test_parse_variable_length_path() {
        let q = Parser::parse("MATCH (a)-[:KNOWS*1..5]->(b) RETURN a, b").unwrap();
        if let Clause::Match { pattern, .. } = &q.clauses[0] {
            let edge = pattern.parts[0].chain.segments[0].edge.as_ref().unwrap();
            assert_eq!(edge.min_hops, Some(1));
            assert_eq!(edge.max_hops, Some(5));
        } else {
            panic!("Expected Match clause");
        }
    }

    #[test]
    fn test_parse_complex_query() {
        let q = Parser::parse(
            "MATCH (a:Person)-[:KNOWS]->(b:Person) \
             WHERE a.age > 30 AND b.active = true \
             RETURN a.name, b.name, count(*) AS mutual_friends \
             ORDER BY mutual_friends DESC \
             LIMIT 10",
        )
        .unwrap();
        assert_eq!(q.clauses.len(), 2); // MATCH + RETURN
    }

    #[test]
    fn test_parse_unwind() {
        let q = Parser::parse("UNWIND [1,2,3] AS x RETURN x").unwrap();
        assert!(matches!(&q.clauses[0], Clause::Unwind { alias, .. } if alias == "x"));
    }

    #[test]
    fn test_parse_with_clause() {
        let q = Parser::parse("MATCH (n) WITH n.name AS name RETURN name").unwrap();
        assert!(matches!(&q.clauses[1], Clause::With { .. }));
    }

    #[test]
    fn test_roundtrip_display() {
        let input = "MATCH (n:Person) WHERE n.age > 30 RETURN n.name";
        let q = Parser::parse(input).unwrap();
        let display = format!("{q}");
        // Display should be parseable
        let q2 = Parser::parse(&display).unwrap();
        assert_eq!(q.clauses.len(), q2.clauses.len());
    }

    #[test]
    fn test_parse_string_predicates() {
        Parser::parse("MATCH (n) WHERE n.name STARTS WITH 'A' RETURN n").unwrap();
        Parser::parse("MATCH (n) WHERE n.name ENDS WITH 'z' RETURN n").unwrap();
    }

    #[test]
    fn test_parse_remove_property_and_label() {
        let query = Parser::parse("MATCH (n) REMOVE n.age, n:Old RETURN n").unwrap();
        assert!(matches!(
            &query.clauses[1],
            Clause::Remove { items } if items.len() == 2
        ));
    }

    #[test]
    fn test_parse_trailing_semicolon() {
        Parser::parse("MATCH (n) RETURN n;").unwrap();
    }

    #[test]
    fn test_parse_undirected_and_open_range_edges() {
        Parser::parse("MATCH (a)-[:KNOWS]-(b) RETURN a, b").unwrap();
        Parser::parse("MATCH (a)-[:KNOWS*..5]->(b) RETURN a, b").unwrap();
    }

    #[test]
    fn test_parse_map_merge_set() {
        Parser::parse("MATCH (n) SET n += {age: 42} RETURN n").unwrap();
    }

    #[test]
    fn test_parse_merge_create() {
        let q = Parser::parse("MERGE (n:Person {name: \"Alice\"})").unwrap();
        assert!(matches!(
            &q.clauses[0],
            Clause::Merge { pattern, on_create, on_match }
            if on_create.is_empty() && on_match.is_empty()
        ));
    }

    #[test]
    fn test_parse_merge_with_on_create() {
        let q = Parser::parse(
            "MERGE (n:Person {name: \"Alice\"}) ON CREATE SET n.age = 30, n.active = true",
        )
        .unwrap();
        if let Clause::Merge { on_create, .. } = &q.clauses[0] {
            assert_eq!(on_create.len(), 2);
        } else {
            panic!("Expected Merge clause");
        }
    }

    #[test]
    fn test_parse_merge_with_on_match() {
        let q = Parser::parse(
            "MERGE (n:Person {name: \"Alice\"}) ON MATCH SET n.lastSeen = timestamp()",
        )
        .unwrap();
        if let Clause::Merge { on_match, .. } = &q.clauses[0] {
            assert_eq!(on_match.len(), 1);
        } else {
            panic!("Expected Merge clause");
        }
    }

    #[test]
    fn test_parse_merge_with_both() {
        let q = Parser::parse(
            "MERGE (n:Person {name: \"Alice\"}) \
             ON CREATE SET n.createdAt = 123, n.score = 0 \
             ON MATCH SET n.lastLogin = 456",
        )
        .unwrap();
        if let Clause::Merge {
            on_create,
            on_match,
            ..
        } = &q.clauses[0]
        {
            assert_eq!(on_create.len(), 2);
            assert_eq!(on_match.len(), 1);
        } else {
            panic!("Expected Merge clause");
        }
    }

    #[test]
    fn test_parse_case_expression() {
        // Simple CASE
        Parser::parse(
            "RETURN CASE n.age WHEN 18 THEN 'teen' WHEN 65 THEN 'senior' ELSE 'adult' END",
        )
        .unwrap();
        // CASE with input expression
        Parser::parse("RETURN CASE n.age WHEN 18 THEN 'teen' ELSE 'adult' END").unwrap();
        // CASE without ELSE
        Parser::parse("RETURN CASE WHEN n.age > 65 THEN 'senior' WHEN n.age > 18 THEN 'adult' END")
            .unwrap();
    }

    #[test]
    fn test_parse_exists_subquery() {
        let q =
            Parser::parse("MATCH (a) WHERE EXISTS { MATCH (a)-[:KNOWS]->(b) } RETURN a").unwrap();
        assert_eq!(q.clauses.len(), 2);
        if let Clause::Match { predicate, .. } = &q.clauses[0] {
            assert!(predicate.is_some());
            assert!(matches!(
                predicate.as_ref().unwrap(),
                Expression::ExistsPattern(_)
            ));
        }
    }

    #[test]
    fn test_parse_list_comprehension() {
        // [x IN list WHERE pred | expr]
        Parser::parse("RETURN [x IN [1,2,3] WHERE x > 1 | x * 2]").unwrap();
        // [x IN list | expr] (no WHERE)
        Parser::parse("RETURN [x IN [1,2,3] | x * 2]").unwrap();
        // [x IN list] (no WHERE, no |)
        Parser::parse("RETURN [x IN [1,2,3]]").unwrap();
    }

    #[test]
    fn test_parse_percentile_functions() {
        Parser::parse("RETURN percentileCont(n.age, 0.5)").unwrap();
        Parser::parse("RETURN percentileDisc(n.age, 0.9)").unwrap();
        Parser::parse("RETURN stDev(n.age), stDevP(n.age)").unwrap();
    }

    #[test]
    fn test_parse_union() {
        let q =
            Parser::parse("MATCH (n:Person) RETURN n.name UNION MATCH (m:Movie) RETURN m.title")
                .unwrap();
        assert_eq!(q.clauses.len(), 1);
        match &q.clauses[0] {
            Clause::Union { all, queries } => {
                assert!(!*all);
                assert_eq!(queries.len(), 2);
                assert_eq!(queries[0].len(), 2); // MATCH + RETURN
                assert_eq!(queries[1].len(), 2);
            }
            other => panic!("Expected Union, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_union_all() {
        let q = Parser::parse(
            "MATCH (n:Person) RETURN n.name UNION ALL MATCH (m:Movie) RETURN m.title",
        )
        .unwrap();
        assert_eq!(q.clauses.len(), 1);
        match &q.clauses[0] {
            Clause::Union { all, queries } => {
                assert!(*all);
                assert_eq!(queries.len(), 2);
            }
            other => panic!("Expected Union, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_triple_union() {
        let q = Parser::parse(
            "MATCH (a:A) RETURN a.x UNION MATCH (b:B) RETURN b.x UNION MATCH (c:C) RETURN c.x",
        )
        .unwrap();
        match &q.clauses[0] {
            Clause::Union { all, queries } => {
                assert!(!*all);
                assert_eq!(queries.len(), 3);
            }
            other => panic!("Expected Union, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_call_subquery() {
        let q = Parser::parse("CALL { MATCH (n) RETURN n }").unwrap();
        assert_eq!(q.clauses.len(), 1);
        match &q.clauses[0] {
            Clause::Call { subquery } => {
                assert_eq!(subquery.len(), 2); // MATCH + RETURN
            }
            other => panic!("Expected Call, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_call_within_query() {
        let q = Parser::parse("MATCH (n) CALL { MATCH (m) RETURN m } RETURN n").unwrap();
        assert_eq!(q.clauses.len(), 3); // Match + Call + Return
        assert!(matches!(&q.clauses[1], Clause::Call { .. }));
    }

    #[test]
    fn test_parse_load_csv() {
        let q = Parser::parse("LOAD CSV FROM 'file:///data.csv' AS row").unwrap();
        assert_eq!(q.clauses.len(), 1);
        match &q.clauses[0] {
            Clause::LoadCsv {
                path,
                alias,
                with_headers,
            } => {
                assert_eq!(path, "file:///data.csv");
                assert_eq!(alias, "row");
                assert!(!*with_headers);
            }
            other => panic!("Expected LoadCsv, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_load_csv_with_headers() {
        let q = Parser::parse("LOAD CSV WITH HEADERS FROM 'file:///data.csv' AS row").unwrap();
        match &q.clauses[0] {
            Clause::LoadCsv { with_headers, .. } => assert!(*with_headers),
            other => panic!("Expected LoadCsv, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_load_csv_with_query() {
        Parser::parse("LOAD CSV FROM 'file:///data.csv' AS row RETURN row").unwrap();
    }

    #[test]
    fn test_union_roundtrip_display() {
        let input = "MATCH (n:Person) RETURN n.name UNION MATCH (m:Movie) RETURN m.title";
        let q = Parser::parse(input).unwrap();
        let display = format!("{q}");
        let q2 = Parser::parse(&display).unwrap();
        assert_eq!(q.clauses.len(), q2.clauses.len());
    }
}

#[cfg(test)]
mod create_tests {
    use super::*;

    #[test]
    fn test_parse_create() {
        let query = "CREATE (n {name: \"Alice\"})";
        let result = Parser::parse(query);
        println!("Parse result: {result:?}");
        assert!(result.is_ok());
    }
}
