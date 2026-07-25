//! Cypher parser for Nexora — lexer, AST, and recursive descent parser.
//!
//! Supports a practical subset of OpenCypher:
//! - MATCH (patterns with nodes and edges)
//! - CREATE / MERGE
//! - SET / REMOVE
//! - DELETE / DETACH DELETE
//! - WITH / RETURN (projections, aggregations)
//! - WHERE (filters)
//! - UNWIND
//! - ORDER BY / SKIP / LIMIT
//! - OPTIONAL MATCH

pub mod aggregate;
pub mod ast;
pub mod evaluator;
pub mod lexer;
pub mod parser;
pub mod semantic;
pub mod token;

pub use aggregate::{aggregator_for, AggregationPipeline, Aggregator};
pub use ast::{Clause, CypherQuery, Expression, Pattern};
pub use evaluator::{EvalContext, EvalError, ExpressionEvaluator};
pub use parser::{ParseError, Parser};
pub use semantic::{SemanticAnalyzer, SemanticError, SemanticInfo};
pub use token::Token;
