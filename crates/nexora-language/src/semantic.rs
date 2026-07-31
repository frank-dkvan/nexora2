//! Semantic analysis for Cypher queries.
//!
//! Validates:
//! - Variable bindings: all referenced variables must be defined
//! - Type compatibility: arithmetic on non-numeric types, etc.
//! - Aggregation function argument validation
//! - Clause ordering: RETURN/UNWIND must have variables in scope

use crate::ast::*;
use std::collections::{HashMap, HashSet};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum SemanticError {
    #[error("variable '{0}' is not defined")]
    UndefinedVariable(String),

    #[error("variable '{0}' is already defined")]
    DuplicateVariable(String),

    #[error("type mismatch: cannot perform {op} on {left_type} and {right_type}")]
    TypeMismatch {
        op: String,
        left_type: String,
        right_type: String,
    },

    #[error("aggregation function {function} cannot be applied to type {value_type}")]
    InvalidAggregationType {
        function: String,
        value_type: String,
    },

    #[error("function {function} requires {expected} arguments, got {actual}")]
    InvalidArgumentCount {
        function: String,
        expected: String,
        actual: usize,
    },

    #[error("RETURN/WITH clause references undefined variables: {variables:?}")]
    ReturnReferencesUndefined { variables: Vec<String> },

    #[error("MERGE pattern must have at most one labeled node per part")]
    InvalidMergePattern,

    #[error("{0}")]
    General(String),
}

/// Result of semantic analysis.
#[derive(Debug, Default)]
pub struct SemanticInfo {
    /// All variables defined by this query.
    pub defined_vars: HashSet<String>,
    /// Variables returned by this query (RETURN clause).
    pub returned_vars: Vec<String>,
    /// Variables available at WITH boundaries.
    pub with_boundaries: Vec<HashSet<String>>,
}

/// Semantic analyzer for Cypher queries.
pub struct SemanticAnalyzer {
    /// Variables currently in scope.
    scope: HashMap<String, VariableInfo>,
    /// Accumulated semantic information.
    info: SemanticInfo,
}

#[derive(Clone, Debug)]
struct VariableInfo {
    /// Where the variable was first defined.
    _defined_in: ClauseKind,
    /// Inferred type (if known).
    #[allow(dead_code)]
    var_type: Option<ValueType>,
}

#[derive(Clone, Debug, PartialEq)]
enum ClauseKind {
    Match,
    Create,
    Merge,
    Unwind,
    With,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum ValueType {
    Integer,
    Float,
    Boolean,
    String,
    List,
    Map,
    Any,
}

impl std::fmt::Display for ValueType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ValueType::Integer => write!(f, "integer"),
            ValueType::Float => write!(f, "float"),
            ValueType::Boolean => write!(f, "boolean"),
            ValueType::String => write!(f, "string"),
            ValueType::List => write!(f, "list"),
            ValueType::Map => write!(f, "map"),
            ValueType::Any => write!(f, "any"),
        }
    }
}

impl SemanticAnalyzer {
    pub fn new() -> Self {
        Self {
            scope: HashMap::new(),
            info: SemanticInfo::default(),
        }
    }

    /// Analyze a Cypher query for semantic correctness.
    pub fn analyze(&mut self, query: &CypherQuery) -> Result<SemanticInfo, SemanticError> {
        for clause in &query.clauses {
            self.analyze_clause(clause)?;
        }
        // Collect defined vars from scope
        for key in self.scope.keys() {
            self.info.defined_vars.insert(key.clone());
        }
        Ok(std::mem::take(&mut self.info))
    }

    fn analyze_clause(&mut self, clause: &Clause) -> Result<(), SemanticError> {
        match clause {
            Clause::Match {
                pattern,
                predicate,
                optional: _,
            } => {
                self.analyze_pattern(pattern, ClauseKind::Match)?;
                if let Some(pred) = predicate {
                    self.check_expression(pred)?;
                }
            }
            Clause::Create { pattern } => {
                self.analyze_pattern(pattern, ClauseKind::Create)?;
            }
            Clause::Merge {
                pattern,
                on_create,
                on_match,
            } => {
                self.analyze_pattern(pattern, ClauseKind::Merge)?;
                for item in on_create {
                    self.check_set_item(item)?;
                }
                for item in on_match {
                    self.check_set_item(item)?;
                }
            }
            Clause::Set { items } => {
                for item in items {
                    self.check_set_item(item)?;
                }
            }
            Clause::Remove { items } => {
                for item in items {
                    self.check_remove_item(item)?;
                }
            }
            Clause::Delete {
                expressions,
                detach: _,
            } => {
                for expr in expressions {
                    self.check_expression(expr)?;
                }
            }
            Clause::With {
                items,
                where_clause,
                order_by,
                skip,
                limit,
                distinct: _,
            } => {
                // WITH is a scope boundary: check expressions against
                // current scope FIRST, then reset to only the WITH projections.
                for item in items {
                    self.check_expression(&item.expression)?;
                }

                let old_keys: HashSet<String> = self.scope.keys().cloned().collect();
                self.info.with_boundaries.push(old_keys);
                self.scope.clear();

                for item in items {
                    let var_name = item
                        .alias
                        .clone()
                        .unwrap_or_else(|| Self::expression_var_name(&item.expression));
                    let info = VariableInfo {
                        _defined_in: ClauseKind::With,
                        var_type: None,
                    };
                    self.scope.insert(var_name, info);
                }

                if let Some(w) = where_clause {
                    self.check_expression(w)?;
                }
                if let Some(o) = order_by {
                    for si in &o.items {
                        self.check_expression(&si.expression)?;
                    }
                }
                if let Some(s) = skip {
                    self.check_expression(s)?;
                }
                if let Some(l) = limit {
                    self.check_expression(l)?;
                }
            }
            Clause::Return {
                items,
                order_by,
                skip,
                limit,
                distinct: _,
            } => {
                for item in items {
                    self.check_expression(&item.expression)?;
                    let var_name = item
                        .alias
                        .clone()
                        .unwrap_or_else(|| Self::expression_var_name(&item.expression));
                    self.info.returned_vars.push(var_name);
                }
                if let Some(o) = order_by {
                    for si in &o.items {
                        self.check_expression(&si.expression)?;
                    }
                }
                if let Some(s) = skip {
                    self.check_expression(s)?;
                }
                if let Some(l) = limit {
                    self.check_expression(l)?;
                }
            }
            Clause::Unwind { expression, alias } => {
                self.check_expression(expression)?;
                if self.scope.contains_key(alias.as_str()) {
                    return Err(SemanticError::DuplicateVariable(alias.clone()));
                }
                self.scope.insert(
                    alias.clone(),
                    VariableInfo {
                        _defined_in: ClauseKind::Unwind,
                        var_type: None,
                    },
                );
            }
            Clause::Union { queries, .. } => {
                for subquery in queries {
                    for clause in subquery {
                        self.analyze_clause(clause)?;
                    }
                }
            }
            Clause::Call { subquery } => {
                for clause in subquery {
                    self.analyze_clause(clause)?;
                }
            }
            Clause::LoadCsv { alias, .. } => {
                if self.scope.contains_key(alias.as_str()) {
                    return Err(SemanticError::DuplicateVariable(alias.clone()));
                }
                self.scope.insert(
                    alias.clone(),
                    VariableInfo {
                        _defined_in: ClauseKind::Unwind,
                        var_type: None,
                    },
                );
            }
        }
        Ok(())
    }

    fn analyze_pattern(
        &mut self,
        pattern: &Pattern,
        kind: ClauseKind,
    ) -> Result<(), SemanticError> {
        for part in &pattern.parts {
            if let Some(ref var) = part.variable {
                self.scope.insert(
                    var.clone(),
                    VariableInfo {
                        _defined_in: kind.clone(),
                        var_type: None,
                    },
                );
            }
            for segment in &part.chain.segments {
                if let Some(ref var) = segment.node.variable {
                    if kind == ClauseKind::Merge && self.scope.contains_key(var.as_str()) {
                        return Err(SemanticError::DuplicateVariable(var.clone()));
                    }
                    self.scope.insert(
                        var.clone(),
                        VariableInfo {
                            _defined_in: kind.clone(),
                            var_type: None,
                        },
                    );
                }
                for (_, value) in &segment.node.properties {
                    self.check_expression(value)?;
                }
                if let Some(ref edge) = segment.edge {
                    if let Some(ref var) = edge.variable {
                        self.scope.insert(
                            var.clone(),
                            VariableInfo {
                                _defined_in: kind.clone(),
                                var_type: None,
                            },
                        );
                    }
                    for (_, value) in &edge.properties {
                        self.check_expression(value)?;
                    }
                }
            }
        }
        Ok(())
    }

    fn check_expression(&self, expr: &Expression) -> Result<(), SemanticError> {
        match expr {
            Expression::Variable(name) => {
                if !self.scope.contains_key(name.as_str()) {
                    return Err(SemanticError::UndefinedVariable(name.clone()));
                }
                Ok(())
            }
            Expression::Property(obj, _) => self.check_expression(obj),
            Expression::Literal(_) => Ok(()),
            Expression::List(items) => {
                for item in items {
                    self.check_expression(item)?;
                }
                Ok(())
            }
            Expression::Map(entries) => {
                for (_, value) in entries {
                    self.check_expression(value)?;
                }
                Ok(())
            }
            Expression::BinOp { op, left, right } => {
                self.check_expression(left)?;
                self.check_expression(right)?;
                self.check_binary_type_compatibility(op, left, right)
            }
            Expression::UnaryOp { op: _, operand } => self.check_expression(operand),
            Expression::Function {
                name,
                args,
                distinct: _,
            } => {
                for arg in args {
                    self.check_expression(arg)?;
                }
                self.check_function_args(name, args)
            }
            Expression::Aggregation {
                function,
                expr: agg_expr,
                distinct: _,
            } => {
                self.check_expression(agg_expr)?;
                self.check_aggregation_type(function, agg_expr)
            }
            Expression::ListComprehension {
                list,
                predicate,
                projection,
                ..
            } => {
                self.check_expression(list)?;
                if let Some(pred) = predicate {
                    self.check_expression(pred)?;
                }
                if let Some(proj) = projection {
                    self.check_expression(proj)?;
                }
                Ok(())
            }
            Expression::Case {
                expr,
                whens,
                else_expr,
            } => {
                if let Some(e) = expr {
                    self.check_expression(e)?;
                }
                for (when, then) in whens {
                    self.check_expression(when)?;
                    self.check_expression(then)?;
                }
                if let Some(e) = else_expr {
                    self.check_expression(e)?;
                }
                Ok(())
            }
            Expression::IsNull(expr) | Expression::IsNotNull(expr) => self.check_expression(expr),
            Expression::ExistsPattern(pattern) => {
                // use a throw-away analyzer for sub-pattern
                let mut sub = SemanticAnalyzer::new();
                sub.analyze_pattern(pattern, ClauseKind::Match)
            }
            Expression::Parenthesized(expr) => self.check_expression(expr),
        }
    }

    fn check_binary_type_compatibility(
        &self,
        op: &BinaryOp,
        left: &Expression,
        right: &Expression,
    ) -> Result<(), SemanticError> {
        let lt = Self::infer_type(left);
        let rt = Self::infer_type(right);

        match op {
            BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div | BinaryOp::Mod => {
                if matches!((&lt, &rt), (ValueType::String, ValueType::String))
                    && *op == BinaryOp::Add
                {
                    return Ok(());
                }
                if !lt.is_numeric() || !rt.is_numeric() {
                    return Err(SemanticError::TypeMismatch {
                        op: format!("{op}"),
                        left_type: lt.to_string(),
                        right_type: rt.to_string(),
                    });
                }
            }
            BinaryOp::StartsWith | BinaryOp::EndsWith | BinaryOp::Contains
                if !matches!(lt, ValueType::String | ValueType::Any)
                    || !matches!(rt, ValueType::String | ValueType::Any) =>
            {
                return Err(SemanticError::TypeMismatch {
                    op: format!("{op}"),
                    left_type: lt.to_string(),
                    right_type: rt.to_string(),
                });
            }
            _ => {}
        }
        Ok(())
    }

    fn check_function_args(&self, name: &str, args: &[Expression]) -> Result<(), SemanticError> {
        let expected = match name.to_lowercase().as_str() {
            "toupper" | "tolower" | "trim" | "tostring" | "tointeger" | "tofloat" | "type"
            | "id" | "size" | "abs" | "round" => 1,
            "coalesce" | "substring" | "split" => 2,
            "replace" => 3,
            _ => return Ok(()),
        };

        if args.len() != expected {
            return Err(SemanticError::InvalidArgumentCount {
                function: name.to_string(),
                expected: expected.to_string(),
                actual: args.len(),
            });
        }
        Ok(())
    }

    fn check_aggregation_type(
        &self,
        function: &AggFunction,
        expr: &Expression,
    ) -> Result<(), SemanticError> {
        let vt = Self::infer_type(expr);
        match function {
            AggFunction::Sum | AggFunction::Avg | AggFunction::StDev | AggFunction::StDevP => {
                if !vt.is_numeric() && vt != ValueType::Any {
                    return Err(SemanticError::InvalidAggregationType {
                        function: format!("{function}"),
                        value_type: vt.to_string(),
                    });
                }
            }
            AggFunction::Count | AggFunction::Min | AggFunction::Max | AggFunction::Collect => {}
            AggFunction::PercentileCont | AggFunction::PercentileDisc => {
                if !vt.is_numeric() && vt != ValueType::Any {
                    return Err(SemanticError::InvalidAggregationType {
                        function: format!("{function}"),
                        value_type: vt.to_string(),
                    });
                }
            }
        }
        Ok(())
    }

    fn check_set_item(&self, item: &SetItem) -> Result<(), SemanticError> {
        match item {
            SetItem::Property { target, value } => {
                self.check_expression(target)?;
                self.check_expression(value)?;
            }
            SetItem::MapProjection { target, map } => {
                self.check_expression(target)?;
                self.check_expression(map)?;
            }
            SetItem::Label { target, label: _ } => {
                self.check_expression(target)?;
            }
        }
        Ok(())
    }

    fn check_remove_item(&self, item: &RemoveItem) -> Result<(), SemanticError> {
        match item {
            RemoveItem::Property { target, key: _ } => {
                self.check_expression(target)?;
            }
            RemoveItem::Label { target, label: _ } => {
                self.check_expression(target)?;
            }
        }
        Ok(())
    }

    fn infer_type(expr: &Expression) -> ValueType {
        match expr {
            Expression::Literal(v) => match v {
                nexora_id::PropertyValue::Integer(_) => ValueType::Integer,
                nexora_id::PropertyValue::Float(_) => ValueType::Float,
                nexora_id::PropertyValue::Boolean(_) => ValueType::Boolean,
                nexora_id::PropertyValue::String(_) => ValueType::String,
                nexora_id::PropertyValue::List(_) => ValueType::List,
                nexora_id::PropertyValue::Map(_) => ValueType::Map,
                nexora_id::PropertyValue::Null => ValueType::Any,
                _ => ValueType::Any,
            },
            Expression::List(_) => ValueType::List,
            Expression::Map(_) => ValueType::Map,
            Expression::BinOp { op, .. } => match op {
                BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div | BinaryOp::Mod => {
                    ValueType::Any
                }
                _ => ValueType::Boolean,
            },
            Expression::UnaryOp { op, .. } => match op {
                UnaryOp::Not => ValueType::Boolean,
                UnaryOp::Neg => ValueType::Any,
            },
            Expression::Function { name, .. } => match name.to_lowercase().as_str() {
                "tostring" | "tolower" | "toupper" | "trim" | "type" | "id" => ValueType::String,
                "tointeger" => ValueType::Integer,
                "tofloat" => ValueType::Float,
                "size" => ValueType::Integer,
                "abs" | "round" => ValueType::Any,
                "coalesce" | "replace" | "split" | "substring" => ValueType::Any,
                _ => ValueType::Any,
            },
            Expression::Aggregation { function, .. } => match function {
                AggFunction::Count => ValueType::Integer,
                AggFunction::Avg | AggFunction::StDev | AggFunction::StDevP => ValueType::Float,
                AggFunction::PercentileCont | AggFunction::PercentileDisc => ValueType::Float,
                AggFunction::Collect => ValueType::List,
                _ => ValueType::Any,
            },
            Expression::Case { .. } => ValueType::Any,
            Expression::IsNull(_) | Expression::IsNotNull(_) => ValueType::Boolean,
            Expression::ExistsPattern(_) => ValueType::Boolean,
            Expression::Parenthesized(e) => Self::infer_type(e),
            _ => ValueType::Any,
        }
    }

    fn expression_var_name(expr: &Expression) -> String {
        match expr {
            Expression::Variable(n) => n.clone(),
            Expression::Property(obj, prop) => match obj.as_ref() {
                Expression::Variable(v) => format!("{v}.{prop}"),
                _ => prop.clone(),
            },
            Expression::Function { name, .. } => format!("<{name}>"),
            _ => String::new(),
        }
    }
}

impl ValueType {
    fn is_numeric(&self) -> bool {
        matches!(self, ValueType::Integer | ValueType::Float | ValueType::Any)
    }
}

impl Default for SemanticAnalyzer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::Parser;

    fn analyze(query: &str) -> Result<SemanticInfo, SemanticError> {
        let parsed = Parser::parse(query).unwrap();
        SemanticAnalyzer::new().analyze(&parsed)
    }

    #[test]
    fn test_valid_variable_binding() {
        let info = analyze("MATCH (a) RETURN a.name").unwrap();
        assert!(info.defined_vars.contains("a"));
    }

    #[test]
    fn test_undefined_variable_in_return() {
        let result = analyze("MATCH (a) RETURN b");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not defined"));
    }

    #[test]
    fn test_undefined_variable_in_where() {
        let result = analyze("MATCH (a) WHERE b.age > 30 RETURN a");
        assert!(result.is_err());
    }

    #[test]
    fn test_valid_create_and_set() {
        let result = analyze("CREATE (n:Person {name: 'Alice'}) SET n.age = 30 RETURN n");
        assert!(result.is_ok());
    }

    #[test]
    fn test_arithmetic_type_check() {
        // String literal + Integer literal should fail type check
        let result = analyze("MATCH (n) WHERE 'hello' + 1 = 'test' RETURN n");
        assert!(result.is_err());
    }

    #[test]
    fn test_string_concatenation_ok() {
        // Both string literals — Add is allowed
        let result = analyze("MATCH (n) WHERE 'hello' + 'world' = 'full' RETURN n");
        assert!(result.is_ok());
    }

    #[test]
    fn test_with_passes_variables() {
        let info = analyze("MATCH (a) WITH a.name AS n RETURN n").unwrap();
        assert!(!info.with_boundaries.is_empty());
    }

    #[test]
    fn test_with_undefined_after_boundary() {
        let result = analyze("MATCH (a) WITH 1 AS x RETURN a");
        assert!(result.is_err());
    }

    #[test]
    fn test_aggregation_sum_on_non_numeric() {
        // String literal inside sum — should fail type check
        let result = analyze("RETURN sum('hello')");
        assert!(result.is_err());
    }

    #[test]
    fn test_aggregation_count_on_any_type() {
        let result = analyze("MATCH (n) RETURN count(n.name)");
        assert!(result.is_ok());
    }

    #[test]
    fn test_function_arg_count() {
        let result = analyze("MATCH (n) RETURN toUpper(n.name, n.age)");
        assert!(result.is_err());
    }

    #[test]
    fn test_valid_merge() {
        let result = analyze("MERGE (p:Person {name: 'Bob'}) ON CREATE SET p.created = true ON MATCH SET p.updated = true RETURN p");
        assert!(result.is_ok());
    }

    #[test]
    fn test_unwind_variable_binding() {
        let info = analyze("UNWIND [1,2,3] AS x RETURN x").unwrap();
        assert!(info.returned_vars.contains(&"x".to_string()));
    }

    #[test]
    fn test_duplicate_variable_in_create() {
        let result = analyze("CREATE (a) CREATE (a)");
        assert!(result.is_ok());
    }

    #[test]
    fn test_case_when_expression() {
        let result = analyze(
            "MATCH (n) RETURN CASE WHEN n.age > 18 THEN 'adult' ELSE 'child' END AS category",
        );
        assert!(result.is_ok());
    }
}
