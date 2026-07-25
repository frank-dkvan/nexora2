//! Cypher AST types.
//!
//! Defines the abstract syntax tree produced by the parser.
//! Each variant corresponds to a Cypher clause or expression type.

use nexora_id::PropertyValue;
use serde::{Deserialize, Serialize};
use std::fmt;

// ============================================================
// Top-level Query
// ============================================================

/// A complete Cypher query: a sequence of clauses.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CypherQuery {
    pub clauses: Vec<Clause>,
    /// Optional time-travel parameter: AS OF timestamp
    pub as_of: Option<u64>,
}

impl CypherQuery {
    pub fn single_clause(&self) -> Option<&Clause> {
        if self.clauses.len() == 1 {
            self.clauses.first()
        } else {
            None
        }
    }
}

impl fmt::Display for CypherQuery {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, clause) in self.clauses.iter().enumerate() {
            if i > 0 {
                write!(f, " ")?;
            }
            write!(f, "{clause}")?;
        }
        Ok(())
    }
}

// ============================================================
// Clauses
// ============================================================

/// A single Cypher clause.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Clause {
    /// MATCH (pattern) WHERE (predicate)
    Match {
        optional: bool,
        pattern: Pattern,
        predicate: Option<Expression>,
    },
    /// CREATE (pattern)
    Create { pattern: Pattern },
    /// MERGE (pattern) ON CREATE SET ... ON MATCH SET ...
    Merge {
        pattern: Pattern,
        on_create: Vec<SetItem>,
        on_match: Vec<SetItem>,
    },
    /// SET expr = expr, ...
    Set { items: Vec<SetItem> },
    /// REMOVE label/property
    Remove { items: Vec<RemoveItem> },
    /// DELETE expr, ...
    Delete {
        expressions: Vec<Expression>,
        detach: bool,
    },
    /// WITH expr AS alias, ...
    With {
        items: Vec<ReturnItem>,
        where_clause: Option<Expression>,
        order_by: Option<OrderBy>,
        skip: Option<Expression>,
        limit: Option<Expression>,
        distinct: bool,
    },
    /// RETURN expr AS alias, ...
    Return {
        items: Vec<ReturnItem>,
        order_by: Option<OrderBy>,
        skip: Option<Expression>,
        limit: Option<Expression>,
        distinct: bool,
    },
    /// UNWIND expr AS alias
    Unwind {
        expression: Expression,
        alias: String,
    },
    /// UNION [ALL] of multiple sub-queries.
    /// Each sub-query is a Vec<Clause>. UNION deduplicates, UNION ALL concatenates.
    Union {
        all: bool,
        queries: Vec<Vec<Clause>>,
    },
    /// CALL { <subquery> } — in-query subquery.
    Call { subquery: Vec<Clause> },
    /// LOAD CSV [WITH HEADERS] FROM 'path' AS alias
    LoadCsv {
        path: String,
        alias: String,
        with_headers: bool,
    },
}

impl fmt::Display for Clause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Match {
                optional,
                pattern,
                predicate,
            } => {
                if *optional {
                    write!(f, "OPTIONAL ")?;
                }
                write!(f, "MATCH {pattern}")?;
                if let Some(pred) = predicate {
                    write!(f, " WHERE {pred}")?;
                }
                Ok(())
            }
            Self::Create { pattern } => write!(f, "CREATE {pattern}"),
            Self::Merge { pattern, .. } => write!(f, "MERGE {pattern}"),
            Self::Set { items } => {
                write!(f, "SET ")?;
                for (i, item) in items.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{item}")?;
                }
                Ok(())
            }
            Self::Remove { items } => {
                write!(f, "REMOVE ")?;
                for (i, item) in items.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{item}")?;
                }
                Ok(())
            }
            Self::Delete {
                expressions,
                detach,
            } => {
                if *detach {
                    write!(f, "DETACH ")?;
                }
                write!(f, "DELETE ")?;
                for (i, expr) in expressions.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{expr}")?;
                }
                Ok(())
            }
            Self::With { items, .. } => {
                write!(f, "WITH ")?;
                for (i, item) in items.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{item}")?;
                }
                Ok(())
            }
            Self::Return { items, .. } => {
                write!(f, "RETURN ")?;
                for (i, item) in items.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{item}")?;
                }
                Ok(())
            }
            Self::Unwind { expression, alias } => {
                write!(f, "UNWIND {expression} AS {alias}")
            }
            Self::Union { all, queries } => {
                for (i, sub) in queries.iter().enumerate() {
                    if i > 0 {
                        write!(f, " UNION ")?;
                        if *all {
                            write!(f, "ALL ")?;
                        }
                    }
                    for (j, clause) in sub.iter().enumerate() {
                        if j > 0 {
                            write!(f, " ")?;
                        }
                        write!(f, "{clause}")?;
                    }
                }
                Ok(())
            }
            Self::Call { subquery } => {
                write!(f, "CALL {{ ")?;
                for (i, clause) in subquery.iter().enumerate() {
                    if i > 0 {
                        write!(f, " ")?;
                    }
                    write!(f, "{clause}")?;
                }
                write!(f, " }}")
            }
            Self::LoadCsv {
                path,
                alias,
                with_headers,
            } => {
                write!(f, "LOAD CSV ")?;
                if *with_headers {
                    write!(f, "WITH HEADERS ")?;
                }
                write!(f, "FROM '{path}' AS {alias}")
            }
        }
    }
}

// ============================================================
// Pattern (graph pattern matching)
// ============================================================

/// A graph pattern: a chain of node-edge-node segments.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Pattern {
    pub parts: Vec<PatternPart>,
}

impl fmt::Display for Pattern {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, part) in self.parts.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{part}")?;
        }
        Ok(())
    }
}

/// A single path in a pattern: `var = (n)-[:TYPE]->(m)`
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PatternPart {
    /// Optional path variable name (e.g., `p` in `MATCH p = (a)-[:KNOWS]->(b)`)
    pub variable: Option<String>,
    /// The chain of nodes and edges.
    pub chain: PatternChain,
}

impl fmt::Display for PatternPart {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(var) = &self.variable {
            write!(f, "{var} = ")?;
        }
        write!(f, "{}", self.chain)
    }
}

/// A chain of alternating nodes and edges: `(a)-[:KNOWS]->(b)-[:IN]->(c)`
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PatternChain {
    pub segments: Vec<PatternSegment>,
}

impl fmt::Display for PatternChain {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, seg) in self.segments.iter().enumerate() {
            if i == 0 {
                write!(f, "{}", seg.node)?;
            }
            if let Some(edge) = &seg.edge {
                write!(f, "{edge}")?;
                if let Some(next_node) = self.segments.get(i + 1).map(|s| &s.node) {
                    write!(f, "{next_node}")?;
                }
            }
        }
        Ok(())
    }
}

/// A segment in a pattern chain: one node + one optional outgoing edge.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PatternSegment {
    pub node: NodePattern,
    pub edge: Option<EdgePattern>,
}

/// A node pattern: `(var:Label {prop: val})`
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct NodePattern {
    pub variable: Option<String>,
    pub labels: Vec<String>,
    pub properties: Vec<(String, Expression)>,
}

impl fmt::Display for NodePattern {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "(")?;
        if let Some(var) = &self.variable {
            write!(f, "{var}")?;
        }
        for label in &self.labels {
            write!(f, ":{label}")?;
        }
        if !self.properties.is_empty() {
            write!(f, " {{")?;
            for (i, (k, v)) in self.properties.iter().enumerate() {
                if i > 0 {
                    write!(f, ", ")?;
                }
                write!(f, "{k}: {v}")?;
            }
            write!(f, "}}")?;
        }
        write!(f, ")")
    }
}

/// Edge direction.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum EdgeDirection {
    Outgoing, // →
    Incoming, // ←
    Either,   // —
}

/// An edge pattern: `-[:TYPE {prop: val}]->`
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EdgePattern {
    pub variable: Option<String>,
    pub edge_type: Option<String>,
    pub properties: Vec<(String, Expression)>,
    pub direction: EdgeDirection,
    /// Variable-length path: `*1..5`
    pub min_hops: Option<u32>,
    pub max_hops: Option<u32>,
}

impl fmt::Display for EdgePattern {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.direction {
            EdgeDirection::Outgoing => write!(f, "-[")?,
            EdgeDirection::Incoming => write!(f, "<-[")?,
            EdgeDirection::Either => write!(f, "-[")?,
        }
        if let Some(var) = &self.variable {
            write!(f, "{var}")?;
        }
        if let Some(typ) = &self.edge_type {
            write!(f, ":{typ}")?;
        }
        if let (Some(min), Some(max)) = (self.min_hops, self.max_hops) {
            write!(f, "*{min}..{max}")?;
        }
        write!(f, "]")?;
        match self.direction {
            EdgeDirection::Outgoing => write!(f, "->"),
            EdgeDirection::Incoming => write!(f, "-"),
            EdgeDirection::Either => write!(f, "-"),
        }
    }
}

// ============================================================
// Expressions
// ============================================================

/// A Cypher expression.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Expression {
    /// Variable reference: `n`, `n.name`
    Variable(String),
    /// Property access: `n.prop`
    Property(Box<Expression>, String),
    /// Literal value
    Literal(PropertyValue),
    /// List literal: `[1, 2, 3]`
    List(Vec<Expression>),
    /// Map literal: {key: val, ...}
    Map(Vec<(String, Expression)>),
    /// Binary operation: `a + b`, `a = b`
    BinOp {
        op: BinaryOp,
        left: Box<Expression>,
        right: Box<Expression>,
    },
    /// Unary operation: `NOT a`, `-a`
    UnaryOp {
        op: UnaryOp,
        operand: Box<Expression>,
    },
    /// Function call: `count(*)`, `toLower(n.name)`
    Function {
        name: String,
        args: Vec<Expression>,
        distinct: bool,
    },
    /// Aggregation: `count(*)`, `avg(n.age)`
    Aggregation {
        function: AggFunction,
        expr: Box<Expression>,
        distinct: bool,
    },
    /// List comprehension: `[x IN list WHERE pred | expr]`
    ListComprehension {
        variable: String,
        list: Box<Expression>,
        predicate: Option<Box<Expression>>,
        projection: Option<Box<Expression>>,
    },
    /// CASE expression
    Case {
        expr: Option<Box<Expression>>,
        whens: Vec<(Expression, Expression)>,
        else_expr: Option<Box<Expression>>,
    },
    /// IS NULL / IS NOT NULL
    IsNull(Box<Expression>),
    IsNotNull(Box<Expression>),
    /// EXISTS { MATCH ... }
    ExistsPattern(Box<Pattern>),
    /// Parenthesized expression
    Parenthesized(Box<Expression>),
}

impl fmt::Display for Expression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Variable(v) => write!(f, "{v}"),
            Self::Property(obj, prop) => write!(f, "{obj}.{prop}"),
            Self::Literal(val) => write!(f, "{val}"),
            Self::BinOp { op, left, right } => write!(f, "({left} {op} {right})"),
            Self::UnaryOp { op, operand } => write!(f, "({op}{operand})"),
            Self::Function {
                name,
                args,
                distinct,
            } => {
                write!(f, "{name}(")?;
                if *distinct {
                    write!(f, "DISTINCT ")?;
                }
                for (i, arg) in args.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{arg}")?;
                }
                write!(f, ")")
            }
            Self::Aggregation {
                function,
                expr,
                distinct,
            } => {
                write!(f, "{function}(")?;
                if *distinct {
                    write!(f, "DISTINCT ")?;
                }
                write!(f, "{expr})")
            }
            Self::IsNull(expr) => write!(f, "{expr} IS NULL"),
            Self::IsNotNull(expr) => write!(f, "{expr} IS NOT NULL"),
            Self::Case {
                expr,
                whens,
                else_expr,
            } => {
                write!(f, "CASE ")?;
                if let Some(e) = expr {
                    write!(f, "{e} ")?;
                }
                for (when, then) in whens {
                    write!(f, "WHEN {when} THEN {then} ")?;
                }
                if let Some(e) = else_expr {
                    write!(f, "ELSE {e} ")?;
                }
                write!(f, "END")
            }
            Self::ExistsPattern(pattern) => write!(f, "EXISTS {{ MATCH {pattern} }}"),
            Self::ListComprehension {
                variable,
                list,
                predicate,
                projection,
            } => {
                write!(f, "[{variable} IN {list}")?;
                if let Some(pred) = predicate {
                    write!(f, " WHERE {pred}")?;
                }
                if let Some(proj) = projection {
                    write!(f, " | {proj}")?;
                }
                write!(f, "]")
            }
            Self::Parenthesized(expr) => write!(f, "({expr})"),
            Self::List(items) => {
                write!(f, "[")?;
                for (i, item) in items.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{item}")?;
                }
                write!(f, "]")
            }
            Self::Map(entries) => {
                write!(f, "{{")?;
                for (i, (k, v)) in entries.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{k}: {v}")?;
                }
                write!(f, "}}")
            }
        }
    }
}

// ============================================================
// Operators
// ============================================================

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum BinaryOp {
    // Comparison
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    // Logical
    And,
    Or,
    Xor,
    // Arithmetic
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    // String
    StartsWith,
    EndsWith,
    Contains,
    // List
    In,
    // Pattern
    RegexMatch,
}

impl fmt::Display for BinaryOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Eq => write!(f, "="),
            Self::Ne => write!(f, "<>"),
            Self::Lt => write!(f, "<"),
            Self::Le => write!(f, "<="),
            Self::Gt => write!(f, ">"),
            Self::Ge => write!(f, ">="),
            Self::And => write!(f, "AND"),
            Self::Or => write!(f, "OR"),
            Self::Xor => write!(f, "XOR"),
            Self::Add => write!(f, "+"),
            Self::Sub => write!(f, "-"),
            Self::Mul => write!(f, "*"),
            Self::Div => write!(f, "/"),
            Self::Mod => write!(f, "%"),
            Self::StartsWith => write!(f, "STARTS WITH"),
            Self::EndsWith => write!(f, "ENDS WITH"),
            Self::Contains => write!(f, "CONTAINS"),
            Self::In => write!(f, "IN"),
            Self::RegexMatch => write!(f, "=~"),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum UnaryOp {
    Not,
    Neg,
}

impl fmt::Display for UnaryOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Not => write!(f, "NOT "),
            Self::Neg => write!(f, "-"),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum AggFunction {
    Count,
    Sum,
    Avg,
    Min,
    Max,
    Collect,
    PercentileCont,
    PercentileDisc,
    StDev,
    StDevP,
}

impl fmt::Display for AggFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Count => write!(f, "count"),
            Self::Sum => write!(f, "sum"),
            Self::Avg => write!(f, "avg"),
            Self::Min => write!(f, "min"),
            Self::Max => write!(f, "max"),
            Self::Collect => write!(f, "collect"),
            Self::PercentileCont => write!(f, "percentileCont"),
            Self::PercentileDisc => write!(f, "percentileDisc"),
            Self::StDev => write!(f, "stDev"),
            Self::StDevP => write!(f, "stDevP"),
        }
    }
}

// ============================================================
// SET / REMOVE items
// ============================================================

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum SetItem {
    /// n.prop = expr
    Property {
        target: Expression,
        value: Expression,
    },
    /// n += {prop: val}
    MapProjection { target: Expression, map: Expression },
    /// n:Label
    Label { target: Expression, label: String },
}

impl fmt::Display for SetItem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Property { target, value } => write!(f, "{target} = {value}"),
            Self::MapProjection { target, map } => write!(f, "{target} += {map}"),
            Self::Label { target, label } => write!(f, "{target}:{label}"),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum RemoveItem {
    /// n.prop
    Property { target: Expression, key: String },
    /// n:Label
    Label { target: Expression, label: String },
}

impl fmt::Display for RemoveItem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Property { target, key } => write!(f, "{target}.{key}"),
            Self::Label { target, label } => write!(f, "{target}:{label}"),
        }
    }
}

// ============================================================
// RETURN / WITH items
// ============================================================

/// A return item: `expr AS alias`
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ReturnItem {
    pub expression: Expression,
    pub alias: Option<String>,
}

impl fmt::Display for ReturnItem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.expression)?;
        if let Some(alias) = &self.alias {
            write!(f, " AS {alias}")?;
        }
        Ok(())
    }
}

/// ORDER BY clause.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct OrderBy {
    pub items: Vec<SortItem>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SortItem {
    pub expression: Expression,
    pub ascending: bool,
}

#[cfg(test)]
mod display_tests {
    use super::*;
    use nexora_id::PropertyValue;

    // A list literal in an expression must render its elements, not the `<expr>`
    // placeholder. The distributed query planner rebuilds an owner query via
    // `to_string()`, so a WHERE like `n.id IN ['a','b']` that rendered as
    // `n.id IN <expr>` was unparseable on the owner and silently broke
    // distributed IN reads/writes.
    #[test]
    fn list_expression_renders_elements_not_placeholder() {
        let list = Expression::List(vec![
            Expression::Literal(PropertyValue::String("a".into())),
            Expression::Literal(PropertyValue::String("b".into())),
        ]);
        let rendered = list.to_string();
        assert!(
            !rendered.contains("<expr>"),
            "list must not render as placeholder: {rendered}"
        );
        assert_eq!(rendered, "[\"a\", \"b\"]");

        // In an IN predicate the whole clause must be re-parseable text.
        let pred = Expression::BinOp {
            op: BinaryOp::In,
            left: Box::new(Expression::Property(
                Box::new(Expression::Variable("n".into())),
                "id".into(),
            )),
            right: Box::new(list),
        };
        assert_eq!(pred.to_string(), "(n.id IN [\"a\", \"b\"])");
    }

    #[test]
    fn map_expression_renders_entries_not_placeholder() {
        let map = Expression::Map(vec![(
            "x".into(),
            Expression::Literal(PropertyValue::Integer(1)),
        )]);
        let rendered = map.to_string();
        assert!(
            !rendered.contains("<expr>"),
            "map must not render as placeholder: {rendered}"
        );
        assert_eq!(rendered, "{x: 1}");
    }
}
