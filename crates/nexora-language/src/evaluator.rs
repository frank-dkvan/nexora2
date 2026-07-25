//! Unified expression evaluator for Cypher expressions.
//!
//! Evaluates AST expressions to PropertyValue results using a variable binding context.

use crate::ast::*;
use nexora_id::PropertyValue;
use std::collections::{BTreeMap, HashMap};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum EvalError {
    #[error("variable '{0}' is not bound")]
    UnboundVariable(String),

    #[error("type error: {0}")]
    TypeError(String),

    #[error("division by zero")]
    DivisionByZero,

    #[error("unknown function: {0}")]
    UnknownFunction(String),

    #[error("function {0}: {1}")]
    FunctionError(String, String),

    #[error("unsupported expression: {0}")]
    Unsupported(String),
}

/// Context for expression evaluation, holding variable bindings.
#[derive(Clone, Debug, Default)]
pub struct EvalContext {
    /// Variable name → current value (for scalar variables).
    pub variables: HashMap<String, PropertyValue>,
    /// Aliased computed properties (e.g., `WITH n.name AS full_name`).
    pub aliases: HashMap<String, PropertyValue>,
}

/// Returns true if a PropertyValue is considered true in boolean context.
fn is_truthy(v: &PropertyValue) -> bool {
    match v {
        PropertyValue::Null => false,
        PropertyValue::Boolean(b) => *b,
        PropertyValue::Integer(i) => *i != 0,
        PropertyValue::Float(f) => *f != 0.0,
        PropertyValue::String(s) => !s.is_empty(),
        PropertyValue::Bytes(b) => !b.is_empty(),
        PropertyValue::List(l) => !l.is_empty(),
        PropertyValue::Map(m) => !m.is_empty(),
        _ => true,
    }
}

impl EvalContext {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_variable(mut self, name: &str, value: PropertyValue) -> Self {
        self.variables.insert(name.to_string(), value);
        self
    }

    pub fn with_variables(mut self, vars: HashMap<String, PropertyValue>) -> Self {
        self.variables.extend(vars);
        self
    }

    /// Resolve a variable, checking aliases first then registered variables.
    pub fn resolve(&self, name: &str) -> Option<&PropertyValue> {
        self.aliases.get(name).or_else(|| self.variables.get(name))
    }
}

/// Maximum recursion depth for expression evaluation, preventing stack
/// overflow on deeply nested expressions (e.g. `(((...1...)))`).
const EVAL_MAX_DEPTH: usize = 256;

/// Unified expression evaluator.
pub struct ExpressionEvaluator;

impl ExpressionEvaluator {
    /// Evaluate an expression within a given context.
    pub fn evaluate(expr: &Expression, ctx: &EvalContext) -> Result<PropertyValue, EvalError> {
        Self::evaluate_with_depth(expr, ctx, 0)
    }

    fn evaluate_with_depth(
        expr: &Expression,
        ctx: &EvalContext,
        depth: usize,
    ) -> Result<PropertyValue, EvalError> {
        if depth > EVAL_MAX_DEPTH {
            return Err(EvalError::Unsupported("expression nesting too deep".into()));
        }
        let next = || depth + 1;
        match expr {
            Expression::Literal(v) => Ok(v.clone()),
            Expression::Variable(name) => ctx
                .resolve(name)
                .cloned()
                .ok_or_else(|| EvalError::UnboundVariable(name.clone())),
            Expression::Property(obj, prop) => {
                let obj_val = Self::evaluate_with_depth(obj, ctx, next())?;
                Self::resolve_property(&obj_val, prop)
            }
            Expression::List(items) => {
                let vals: Result<Vec<_>, _> = items
                    .iter()
                    .map(|e| Self::evaluate_with_depth(e, ctx, next()))
                    .collect();
                Ok(PropertyValue::List(vals?))
            }
            Expression::Map(entries) => {
                let map: BTreeMap<String, PropertyValue> = entries
                    .iter()
                    .map(|(k, e)| {
                        let v = Self::evaluate_with_depth(e, ctx, next())?;
                        Ok((k.clone(), v))
                    })
                    .collect::<Result<_, EvalError>>()?;
                Ok(PropertyValue::Map(map))
            }
            Expression::BinOp { op, left, right } => {
                // Short-circuit evaluation for AND and OR
                match op {
                    BinaryOp::And => {
                        let lhs = Self::evaluate_with_depth(left, ctx, next())?;
                        if !is_truthy(&lhs) {
                            // Short-circuit: left is false, don't evaluate right
                            return Ok(PropertyValue::Boolean(false));
                        }
                        let rhs = Self::evaluate_with_depth(right, ctx, next())?;
                        Ok(PropertyValue::Boolean(is_truthy(&rhs)))
                    }
                    BinaryOp::Or => {
                        let lhs = Self::evaluate_with_depth(left, ctx, next())?;
                        if is_truthy(&lhs) {
                            // Short-circuit: left is true, don't evaluate right
                            return Ok(PropertyValue::Boolean(true));
                        }
                        let rhs = Self::evaluate_with_depth(right, ctx, next())?;
                        Ok(PropertyValue::Boolean(is_truthy(&rhs)))
                    }
                    _ => {
                        // For all other operators, evaluate both sides
                        let lhs = Self::evaluate_with_depth(left, ctx, next())?;
                        let rhs = Self::evaluate_with_depth(right, ctx, next())?;
                        Self::eval_binary_op(op, &lhs, &rhs)
                    }
                }
            }
            Expression::UnaryOp { op, operand } => {
                let val = Self::evaluate_with_depth(operand, ctx, next())?;
                Self::eval_unary_op(op, &val)
            }
            Expression::Function {
                name,
                args,
                distinct: _,
            } => {
                let evaluated: Result<Vec<_>, _> = args
                    .iter()
                    .map(|a| Self::evaluate_with_depth(a, ctx, next()))
                    .collect();
                Self::eval_function(name, &evaluated?)
            }
            Expression::Aggregation { .. } => Err(EvalError::Unsupported(
                "aggregation not supported in scalar context; use aggregation pipeline".into(),
            )),
            Expression::ListComprehension {
                variable,
                list,
                predicate,
                projection,
            } => {
                let list_val = Self::evaluate_with_depth(list, ctx, next())?;
                let items = match &list_val {
                    PropertyValue::List(items) => items.clone(),
                    other => {
                        return Err(EvalError::TypeError(format!(
                            "list comprehension requires a list, got: {other}"
                        )))
                    }
                };
                let mut result = Vec::new();
                for item in &items {
                    let mut inner_ctx = ctx.clone();
                    inner_ctx.variables.insert(variable.clone(), item.clone());
                    let pass = predicate
                        .as_ref()
                        .map(|p| match Self::evaluate_with_depth(p, &inner_ctx, next()) {
                            Ok(v) => is_truthy(&v),
                            Err(_) => false,
                        })
                        .unwrap_or(true);
                    if pass {
                        let val = if let Some(proj) = projection {
                            Self::evaluate_with_depth(proj, &inner_ctx, next())?
                        } else {
                            item.clone()
                        };
                        result.push(val);
                    }
                }
                Ok(PropertyValue::List(result))
            }
            Expression::Case {
                expr,
                whens,
                else_expr,
            } => {
                let test_val = expr
                    .as_ref()
                    .map(|e| Self::evaluate_with_depth(e, ctx, next()))
                    .transpose()?;
                for (when, then) in whens {
                    let when_val = Self::evaluate_with_depth(when, ctx, next())?;
                    let condition = if let Some(ref test) = test_val {
                        // Simple CASE: compare when_val == test
                        test == &when_val
                    } else {
                        // Searched CASE: evaluate when as boolean
                        is_truthy(&when_val)
                    };
                    if condition {
                        return Self::evaluate_with_depth(then, ctx, next());
                    }
                }
                if let Some(else_e) = else_expr {
                    Self::evaluate_with_depth(else_e, ctx, next())
                } else {
                    Ok(PropertyValue::Null)
                }
            }
            Expression::IsNull(expr) => {
                let val = Self::evaluate_with_depth(expr, ctx, next())?;
                Ok(PropertyValue::Boolean(val.is_null()))
            }
            Expression::IsNotNull(expr) => {
                let val = Self::evaluate_with_depth(expr, ctx, next())?;
                Ok(PropertyValue::Boolean(!val.is_null()))
            }
            Expression::ExistsPattern(_) => Err(EvalError::Unsupported(
                "EXISTS pattern not supported in scalar evaluator".into(),
            )),
            Expression::Parenthesized(expr) => Self::evaluate_with_depth(expr, ctx, next()),
        }
    }

    // ---- Binary Operations ----

    fn eval_binary_op(
        op: &BinaryOp,
        lhs: &PropertyValue,
        rhs: &PropertyValue,
    ) -> Result<PropertyValue, EvalError> {
        match op {
            // Comparison
            BinaryOp::Eq => Ok(PropertyValue::Boolean(lhs == rhs)),
            BinaryOp::Ne => Ok(PropertyValue::Boolean(lhs != rhs)),
            BinaryOp::Lt => Self::compare(lhs, rhs, std::cmp::Ordering::Less, false),
            BinaryOp::Le => Self::compare(lhs, rhs, std::cmp::Ordering::Less, true),
            BinaryOp::Gt => Self::compare(lhs, rhs, std::cmp::Ordering::Greater, false),
            BinaryOp::Ge => Self::compare(lhs, rhs, std::cmp::Ordering::Greater, true),
            // Logical
            BinaryOp::And => Ok(PropertyValue::Boolean(is_truthy(lhs) && is_truthy(rhs))),
            BinaryOp::Or => Ok(PropertyValue::Boolean(is_truthy(lhs) || is_truthy(rhs))),
            BinaryOp::Xor => Ok(PropertyValue::Boolean(is_truthy(lhs) != is_truthy(rhs))),
            // Arithmetic
            BinaryOp::Add => Self::arith_add(lhs, rhs),
            BinaryOp::Sub => Self::arith_sub(lhs, rhs),
            BinaryOp::Mul => Self::arith_mul(lhs, rhs),
            BinaryOp::Div => Self::arith_div(lhs, rhs),
            BinaryOp::Mod => Self::arith_mod(lhs, rhs),
            // String
            BinaryOp::StartsWith => {
                let s = lhs.as_str().unwrap_or_default();
                let prefix = rhs.as_str().unwrap_or_default();
                Ok(PropertyValue::Boolean(s.starts_with(prefix)))
            }
            BinaryOp::EndsWith => {
                let s = lhs.as_str().unwrap_or_default();
                let suffix = rhs.as_str().unwrap_or_default();
                Ok(PropertyValue::Boolean(s.ends_with(suffix)))
            }
            BinaryOp::Contains => {
                let s = lhs.as_str().unwrap_or_default();
                let substr = rhs.as_str().unwrap_or_default();
                Ok(PropertyValue::Boolean(s.contains(substr)))
            }
            // List
            BinaryOp::In => match rhs {
                PropertyValue::List(items) => Ok(PropertyValue::Boolean(items.contains(lhs))),
                other => Err(EvalError::TypeError(format!(
                    "IN requires a list on the right side, got: {other}"
                ))),
            },
            // Regex
            BinaryOp::RegexMatch => {
                let s = lhs.as_str().unwrap_or_default();
                let pattern = rhs.as_str().unwrap_or_default();
                match regex::Regex::new(pattern) {
                    Ok(re) => Ok(PropertyValue::Boolean(re.is_match(s))),
                    Err(e) => Err(EvalError::FunctionError(
                        "REGEX".into(),
                        format!("invalid regex: {e}"),
                    )),
                }
            }
        }
    }

    fn compare(
        lhs: &PropertyValue,
        rhs: &PropertyValue,
        expected: std::cmp::Ordering,
        or_equal: bool,
    ) -> Result<PropertyValue, EvalError> {
        let ordering = Self::try_compare(lhs, rhs)?;
        let result = match expected {
            std::cmp::Ordering::Less if or_equal => {
                ordering == std::cmp::Ordering::Less || ordering == std::cmp::Ordering::Equal
            }
            std::cmp::Ordering::Less => ordering == std::cmp::Ordering::Less,
            std::cmp::Ordering::Greater if or_equal => {
                ordering == std::cmp::Ordering::Greater || ordering == std::cmp::Ordering::Equal
            }
            std::cmp::Ordering::Greater => ordering == std::cmp::Ordering::Greater,
            std::cmp::Ordering::Equal => ordering == std::cmp::Ordering::Equal,
        };
        Ok(PropertyValue::Boolean(result))
    }

    fn try_compare(
        lhs: &PropertyValue,
        rhs: &PropertyValue,
    ) -> Result<std::cmp::Ordering, EvalError> {
        // Coerce floats and integers
        if let (Some(lf), Some(rf)) = (lhs.as_f64(), rhs.as_f64()) {
            return Ok(lf.total_cmp(&rf));
        }
        // Fall back to string comparison
        let ls = lhs.to_string();
        let rs = rhs.to_string();
        Ok(ls.cmp(&rs))
    }

    fn arith_add(lhs: &PropertyValue, rhs: &PropertyValue) -> Result<PropertyValue, EvalError> {
        match (lhs, rhs) {
            (PropertyValue::Integer(a), PropertyValue::Integer(b)) => Ok(PropertyValue::Integer(
                a.checked_add(*b)
                    .ok_or_else(|| EvalError::TypeError(format!("addition overflow: {a} + {b}")))?,
            )),
            (PropertyValue::Float(a), PropertyValue::Float(b)) => Ok(PropertyValue::Float(a + b)),
            (PropertyValue::Integer(a), PropertyValue::Float(b)) => {
                Ok(PropertyValue::Float(*a as f64 + b))
            }
            (PropertyValue::Float(a), PropertyValue::Integer(b)) => {
                Ok(PropertyValue::Float(a + *b as f64))
            }
            (PropertyValue::String(a), PropertyValue::String(b)) => {
                Ok(PropertyValue::String(format!("{a}{b}")))
            }
            (PropertyValue::String(a), other) => Ok(PropertyValue::String(format!("{a}{other}"))),
            (other, PropertyValue::String(b)) => Ok(PropertyValue::String(format!("{other}{b}"))),
            (PropertyValue::List(a), PropertyValue::List(b)) => {
                let mut combined = a.clone();
                combined.extend(b.iter().cloned());
                Ok(PropertyValue::List(combined))
            }
            (a, b) => Err(EvalError::TypeError(format!("cannot add {a} and {b}"))),
        }
    }

    fn arith_sub(lhs: &PropertyValue, rhs: &PropertyValue) -> Result<PropertyValue, EvalError> {
        match (lhs, rhs) {
            (PropertyValue::Integer(a), PropertyValue::Integer(b)) => {
                Ok(PropertyValue::Integer(a.checked_sub(*b).ok_or_else(
                    || EvalError::TypeError(format!("subtraction overflow: {a} - {b}")),
                )?))
            }
            (PropertyValue::Float(a), PropertyValue::Float(b)) => Ok(PropertyValue::Float(a - b)),
            (PropertyValue::Integer(a), PropertyValue::Float(b)) => {
                Ok(PropertyValue::Float(*a as f64 - b))
            }
            (PropertyValue::Float(a), PropertyValue::Integer(b)) => {
                Ok(PropertyValue::Float(a - *b as f64))
            }
            (a, b) => Err(EvalError::TypeError(format!("cannot subtract {a} and {b}"))),
        }
    }

    fn arith_mul(lhs: &PropertyValue, rhs: &PropertyValue) -> Result<PropertyValue, EvalError> {
        match (lhs, rhs) {
            (PropertyValue::Integer(a), PropertyValue::Integer(b)) => {
                Ok(PropertyValue::Integer(a.checked_mul(*b).ok_or_else(
                    || EvalError::TypeError(format!("multiplication overflow: {a} * {b}")),
                )?))
            }
            (PropertyValue::Float(a), PropertyValue::Float(b)) => Ok(PropertyValue::Float(a * b)),
            (PropertyValue::Integer(a), PropertyValue::Float(b)) => {
                Ok(PropertyValue::Float(*a as f64 * b))
            }
            (PropertyValue::Float(a), PropertyValue::Integer(b)) => {
                Ok(PropertyValue::Float(a * *b as f64))
            }
            (a, b) => Err(EvalError::TypeError(format!("cannot multiply {a} and {b}"))),
        }
    }

    fn arith_div(lhs: &PropertyValue, rhs: &PropertyValue) -> Result<PropertyValue, EvalError> {
        let (a_f, b_f) = Self::to_float_pair(lhs, rhs)?;
        if b_f == 0.0 {
            return Err(EvalError::DivisionByZero);
        }
        let result = a_f / b_f;
        // Return integer if both are integers and exact division
        if matches!(lhs, PropertyValue::Integer(_))
            && matches!(rhs, PropertyValue::Integer(_))
            && result == result.trunc()
        {
            return Ok(PropertyValue::Integer(result as i64));
        }
        Ok(PropertyValue::Float(result))
    }

    fn arith_mod(lhs: &PropertyValue, rhs: &PropertyValue) -> Result<PropertyValue, EvalError> {
        match (lhs, rhs) {
            (PropertyValue::Integer(a), PropertyValue::Integer(b)) if *b != 0 => {
                Ok(PropertyValue::Integer(a % b))
            }
            (PropertyValue::Integer(_), PropertyValue::Integer(_)) => {
                Err(EvalError::DivisionByZero)
            }
            (a, b) => Err(EvalError::TypeError(format!(
                "modulo requires integers, got {a} and {b}"
            ))),
        }
    }

    fn to_float_pair(lhs: &PropertyValue, rhs: &PropertyValue) -> Result<(f64, f64), EvalError> {
        let a_f = lhs
            .as_f64()
            .ok_or_else(|| EvalError::TypeError(format!("not a number: {lhs}")))?;
        let b_f = rhs
            .as_f64()
            .ok_or_else(|| EvalError::TypeError(format!("not a number: {rhs}")))?;
        Ok((a_f, b_f))
    }

    // ---- Unary Operations ----

    fn eval_unary_op(op: &UnaryOp, val: &PropertyValue) -> Result<PropertyValue, EvalError> {
        match op {
            UnaryOp::Not => Ok(PropertyValue::Boolean(!is_truthy(val))),
            UnaryOp::Neg => match val {
                PropertyValue::Integer(i) => {
                    Ok(PropertyValue::Integer(i.checked_neg().ok_or_else(
                        || EvalError::TypeError(format!("integer negation overflow: -{i}")),
                    )?))
                }
                PropertyValue::Float(f) => Ok(PropertyValue::Float(-f)),
                other => Err(EvalError::TypeError(format!("cannot negate {other}"))),
            },
        }
    }

    // ---- Scalar Functions ----

    fn eval_function(name: &str, args: &[PropertyValue]) -> Result<PropertyValue, EvalError> {
        match name.to_lowercase().as_str() {
            "tostring" => {
                ensure_arg_count(name, args, 1)?;
                Ok(PropertyValue::String(format!("{}", args[0])))
            }
            "tointeger" => {
                ensure_arg_count(name, args, 1)?;
                match &args[0] {
                    PropertyValue::Integer(i) => Ok(PropertyValue::Integer(*i)),
                    PropertyValue::Float(f) => Ok(PropertyValue::Integer(*f as i64)),
                    PropertyValue::String(s) => {
                        s.parse::<i64>().map(PropertyValue::Integer).map_err(|_| {
                            EvalError::FunctionError(
                                "toInteger".into(),
                                format!("invalid integer string: '{s}'"),
                            )
                        })
                    }
                    PropertyValue::Boolean(b) => Ok(PropertyValue::Integer(if *b { 1 } else { 0 })),
                    other => Err(EvalError::FunctionError(
                        "toInteger".into(),
                        format!("cannot convert {other} to integer"),
                    )),
                }
            }
            "tofloat" => {
                ensure_arg_count(name, args, 1)?;
                match &args[0] {
                    PropertyValue::Float(f) => Ok(PropertyValue::Float(*f)),
                    PropertyValue::Integer(i) => Ok(PropertyValue::Float(*i as f64)),
                    PropertyValue::String(s) => {
                        s.parse::<f64>().map(PropertyValue::Float).map_err(|_| {
                            EvalError::FunctionError(
                                "toFloat".into(),
                                format!("invalid float string: '{s}'"),
                            )
                        })
                    }
                    other => Err(EvalError::FunctionError(
                        "toFloat".into(),
                        format!("cannot convert {other} to float"),
                    )),
                }
            }
            "toupper" => {
                ensure_arg_count(name, args, 1)?;
                let s = args[0].as_str().unwrap_or_default();
                Ok(PropertyValue::String(s.to_uppercase()))
            }
            "tolower" => {
                ensure_arg_count(name, args, 1)?;
                let s = args[0].as_str().unwrap_or_default();
                Ok(PropertyValue::String(s.to_lowercase()))
            }
            "trim" => {
                ensure_arg_count(name, args, 1)?;
                let s = args[0].as_str().unwrap_or_default();
                Ok(PropertyValue::String(s.trim().to_string()))
            }
            "substring" => {
                ensure_arg_count(name, args, 2)?;
                let s = args[0].as_str().unwrap_or_default();
                let start = match &args[1] {
                    PropertyValue::Integer(i) => *i as usize,
                    _ => 0,
                };
                // Clamp the start to a valid char boundary so that
                // `substring("café", 4)` does not panic on a non-char-boundary
                // byte index. We use `char_indices` to find the nearest boundary.
                let result = if let Some(pos) = s
                    .char_indices()
                    .map(|(i, _)| i)
                    .chain(std::iter::once(s.len()))
                    .find(|&i| i >= start)
                {
                    s[pos..].to_string()
                } else {
                    String::new()
                };
                Ok(PropertyValue::String(result))
            }
            "coalesce" => {
                ensure_arg_count(name, args, 2)?;
                if args[0].is_null() {
                    Ok(args[1].clone())
                } else {
                    Ok(args[0].clone())
                }
            }
            "type" => {
                ensure_arg_count(name, args, 1)?;
                let type_str = match &args[0] {
                    PropertyValue::Null => "null",
                    PropertyValue::Boolean(_) => "boolean",
                    PropertyValue::Integer(_) => "integer",
                    PropertyValue::Float(_) => "float",
                    PropertyValue::String(_) => "string",
                    PropertyValue::Bytes(_) => "bytes",
                    PropertyValue::List(_) => "list",
                    PropertyValue::Map(_) => "map",
                    PropertyValue::Node(_) => "node",
                    PropertyValue::Relationship(_) => "relationship",
                    PropertyValue::Path(_) => "path",
                    PropertyValue::Date(_) => "date",
                    PropertyValue::LocalDateTime(_) | PropertyValue::ZonedDateTime(_) => "datetime",
                    PropertyValue::Duration(_) => "duration",
                    PropertyValue::Point(_) => "point",
                    PropertyValue::BlobRef(_) => "blobref",
                };
                Ok(PropertyValue::String(type_str.to_string()))
            }
            "id" => {
                ensure_arg_count(name, args, 1)?;
                Ok(args[0].clone())
            }
            "size" => {
                ensure_arg_count(name, args, 1)?;
                match &args[0] {
                    PropertyValue::String(s) => {
                        Ok(PropertyValue::Integer(s.chars().count() as i64))
                    }
                    PropertyValue::List(items) => Ok(PropertyValue::Integer(items.len() as i64)),
                    other => Err(EvalError::FunctionError(
                        "size".into(),
                        format!("size requires a string or list, got {other}"),
                    )),
                }
            }
            "replace" => {
                ensure_arg_count(name, args, 3)?;
                let s = args[0].as_str().unwrap_or_default();
                let from = args[1].as_str().unwrap_or_default();
                let to = args[2].as_str().unwrap_or_default();
                Ok(PropertyValue::String(s.replace(from, to)))
            }
            "split" => {
                ensure_arg_count(name, args, 2)?;
                let s = args[0].as_str().unwrap_or_default();
                let delim = args[1].as_str().unwrap_or_default();
                let parts: Vec<PropertyValue> = s
                    .split(delim)
                    .map(|p| PropertyValue::String(p.to_string()))
                    .collect();
                Ok(PropertyValue::List(parts))
            }
            "abs" => {
                ensure_arg_count(name, args, 1)?;
                match &args[0] {
                    PropertyValue::Integer(i) => Ok(PropertyValue::Integer(i.abs())),
                    PropertyValue::Float(f) => Ok(PropertyValue::Float(f.abs())),
                    other => Err(EvalError::FunctionError(
                        "abs".into(),
                        format!("abs requires a number, got {other}"),
                    )),
                }
            }
            "round" => {
                ensure_arg_count(name, args, 1)?;
                match &args[0] {
                    PropertyValue::Float(f) => Ok(PropertyValue::Float(f.round())),
                    PropertyValue::Integer(i) => Ok(PropertyValue::Integer(*i)),
                    other => Err(EvalError::FunctionError(
                        "round".into(),
                        format!("round requires a number, got {other}"),
                    )),
                }
            }
            _ => Err(EvalError::UnknownFunction(name.to_string())),
        }
    }

    // ---- Property access ----

    fn resolve_property(obj: &PropertyValue, prop: &str) -> Result<PropertyValue, EvalError> {
        match obj {
            PropertyValue::Map(map) => Ok(map.get(prop).cloned().unwrap_or(PropertyValue::Null)),
            _ => Ok(PropertyValue::Null),
        }
    }
}

fn ensure_arg_count(name: &str, args: &[PropertyValue], expected: usize) -> Result<(), EvalError> {
    if args.len() != expected {
        Err(EvalError::FunctionError(
            name.to_string(),
            format!("expected {expected} arguments, got {}", args.len()),
        ))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn eval(expr: &Expression, ctx: &EvalContext) -> Result<PropertyValue, EvalError> {
        ExpressionEvaluator::evaluate(expr, ctx)
    }

    fn lit(val: PropertyValue) -> Expression {
        Expression::Literal(val)
    }

    fn var(name: &str) -> Expression {
        Expression::Variable(name.to_string())
    }

    #[test]
    fn test_literal() {
        let result = eval(&lit(PropertyValue::Integer(42)), &EvalContext::new()).unwrap();
        assert_eq!(result, PropertyValue::Integer(42));
    }

    #[test]
    fn test_variable_resolution() {
        let ctx = EvalContext::new().with_variable("x", PropertyValue::Integer(99));
        let result = eval(&var("x"), &ctx).unwrap();
        assert_eq!(result, PropertyValue::Integer(99));
    }

    #[test]
    fn test_unbound_variable() {
        let result = eval(&var("unknown"), &EvalContext::new());
        assert!(result.is_err());
    }

    #[test]
    fn test_binary_add_integers() {
        let expr = Expression::BinOp {
            op: BinaryOp::Add,
            left: Box::new(lit(PropertyValue::Integer(10))),
            right: Box::new(lit(PropertyValue::Integer(20))),
        };
        assert_eq!(
            eval(&expr, &EvalContext::new()).unwrap(),
            PropertyValue::Integer(30)
        );
    }

    #[test]
    fn test_binary_eq_true() {
        let expr = Expression::BinOp {
            op: BinaryOp::Eq,
            left: Box::new(lit(PropertyValue::Integer(5))),
            right: Box::new(lit(PropertyValue::Integer(5))),
        };
        assert_eq!(
            eval(&expr, &EvalContext::new()).unwrap(),
            PropertyValue::Boolean(true)
        );
    }

    #[test]
    fn test_binary_gt_false() {
        let expr = Expression::BinOp {
            op: BinaryOp::Gt,
            left: Box::new(lit(PropertyValue::Integer(3))),
            right: Box::new(lit(PropertyValue::Integer(10))),
        };
        assert_eq!(
            eval(&expr, &EvalContext::new()).unwrap(),
            PropertyValue::Boolean(false)
        );
    }

    #[test]
    fn test_string_concat() {
        let expr = Expression::BinOp {
            op: BinaryOp::Add,
            left: Box::new(lit(PropertyValue::String("Hello ".into()))),
            right: Box::new(lit(PropertyValue::String("World".into()))),
        };
        assert_eq!(
            eval(&expr, &EvalContext::new()).unwrap(),
            PropertyValue::String("Hello World".into())
        );
    }

    #[test]
    fn test_starts_with() {
        let expr = Expression::BinOp {
            op: BinaryOp::StartsWith,
            left: Box::new(lit(PropertyValue::String("hello world".into()))),
            right: Box::new(lit(PropertyValue::String("hello".into()))),
        };
        assert_eq!(
            eval(&expr, &EvalContext::new()).unwrap(),
            PropertyValue::Boolean(true)
        );
    }

    #[test]
    fn test_contains() {
        let expr = Expression::BinOp {
            op: BinaryOp::Contains,
            left: Box::new(lit(PropertyValue::String("hello world".into()))),
            right: Box::new(lit(PropertyValue::String("lo w".into()))),
        };
        assert_eq!(
            eval(&expr, &EvalContext::new()).unwrap(),
            PropertyValue::Boolean(true)
        );
    }

    #[test]
    fn test_list_in() {
        let expr = Expression::BinOp {
            op: BinaryOp::In,
            left: Box::new(lit(PropertyValue::Integer(3))),
            right: Box::new(lit(PropertyValue::List(vec![
                PropertyValue::Integer(1),
                PropertyValue::Integer(2),
                PropertyValue::Integer(3),
            ]))),
        };
        assert_eq!(
            eval(&expr, &EvalContext::new()).unwrap(),
            PropertyValue::Boolean(true)
        );
    }

    #[test]
    fn test_unary_not() {
        let expr = Expression::UnaryOp {
            op: UnaryOp::Not,
            operand: Box::new(lit(PropertyValue::Boolean(true))),
        };
        assert_eq!(
            eval(&expr, &EvalContext::new()).unwrap(),
            PropertyValue::Boolean(false)
        );
    }

    #[test]
    fn test_function_to_upper() {
        let expr = Expression::Function {
            name: "toUpper".to_string(),
            args: vec![lit(PropertyValue::String("hello".into()))],
            distinct: false,
        };
        assert_eq!(
            eval(&expr, &EvalContext::new()).unwrap(),
            PropertyValue::String("HELLO".into())
        );
    }

    #[test]
    fn test_function_coalesce_first() {
        let expr = Expression::Function {
            name: "coalesce".to_string(),
            args: vec![
                lit(PropertyValue::Integer(42)),
                lit(PropertyValue::Integer(0)),
            ],
            distinct: false,
        };
        assert_eq!(
            eval(&expr, &EvalContext::new()).unwrap(),
            PropertyValue::Integer(42)
        );
    }

    #[test]
    fn test_function_coalesce_null() {
        let expr = Expression::Function {
            name: "coalesce".to_string(),
            args: vec![
                lit(PropertyValue::Null),
                lit(PropertyValue::String("fallback".into())),
            ],
            distinct: false,
        };
        assert_eq!(
            eval(&expr, &EvalContext::new()).unwrap(),
            PropertyValue::String("fallback".into())
        );
    }

    #[test]
    fn test_case_simple() {
        let expr = Expression::Case {
            expr: Some(Box::new(var("score"))),
            whens: vec![(
                lit(PropertyValue::Integer(100)),
                lit(PropertyValue::String("perfect".into())),
            )],
            else_expr: Some(Box::new(lit(PropertyValue::String("not perfect".into())))),
        };
        let ctx = EvalContext::new().with_variable("score", PropertyValue::Integer(100));
        assert_eq!(
            eval(&expr, &ctx).unwrap(),
            PropertyValue::String("perfect".into())
        );
    }

    #[test]
    fn test_case_searched() {
        let expr = Expression::Case {
            expr: None,
            whens: vec![(
                Expression::BinOp {
                    op: BinaryOp::Gt,
                    left: Box::new(var("age")),
                    right: Box::new(lit(PropertyValue::Integer(18))),
                },
                lit(PropertyValue::String("adult".into())),
            )],
            else_expr: Some(Box::new(lit(PropertyValue::String("child".into())))),
        };
        let ctx = EvalContext::new().with_variable("age", PropertyValue::Integer(25));
        assert_eq!(
            eval(&expr, &ctx).unwrap(),
            PropertyValue::String("adult".into())
        );
    }

    #[test]
    fn test_is_null() {
        let expr = Expression::IsNull(Box::new(lit(PropertyValue::Null)));
        assert_eq!(
            eval(&expr, &EvalContext::new()).unwrap(),
            PropertyValue::Boolean(true)
        );
    }

    #[test]
    fn test_is_not_null() {
        let expr = Expression::IsNotNull(Box::new(lit(PropertyValue::Integer(1))));
        assert_eq!(
            eval(&expr, &EvalContext::new()).unwrap(),
            PropertyValue::Boolean(true)
        );
    }

    #[test]
    fn test_division_by_zero() {
        let expr = Expression::BinOp {
            op: BinaryOp::Div,
            left: Box::new(lit(PropertyValue::Integer(10))),
            right: Box::new(lit(PropertyValue::Integer(0))),
        };
        assert!(matches!(
            eval(&expr, &EvalContext::new()),
            Err(EvalError::DivisionByZero)
        ));
    }

    #[test]
    fn test_function_trim() {
        let expr = Expression::Function {
            name: "trim".to_string(),
            args: vec![lit(PropertyValue::String("  hello  ".into()))],
            distinct: false,
        };
        assert_eq!(
            eval(&expr, &EvalContext::new()).unwrap(),
            PropertyValue::String("hello".into())
        );
    }

    #[test]
    fn test_function_abs() {
        let expr = Expression::Function {
            name: "abs".to_string(),
            args: vec![lit(PropertyValue::Integer(-42))],
            distinct: false,
        };
        assert_eq!(
            eval(&expr, &EvalContext::new()).unwrap(),
            PropertyValue::Integer(42)
        );
    }

    #[test]
    fn test_and_logical() {
        let expr = Expression::BinOp {
            op: BinaryOp::And,
            left: Box::new(lit(PropertyValue::Boolean(true))),
            right: Box::new(lit(PropertyValue::Boolean(true))),
        };
        assert_eq!(
            eval(&expr, &EvalContext::new()).unwrap(),
            PropertyValue::Boolean(true)
        );
    }

    #[test]
    fn test_float_division() {
        let expr = Expression::BinOp {
            op: BinaryOp::Div,
            left: Box::new(lit(PropertyValue::Float(10.0))),
            right: Box::new(lit(PropertyValue::Float(4.0))),
        };
        assert_eq!(
            eval(&expr, &EvalContext::new()).unwrap(),
            PropertyValue::Float(2.5)
        );
    }

    #[test]
    fn test_modulo() {
        let expr = Expression::BinOp {
            op: BinaryOp::Mod,
            left: Box::new(lit(PropertyValue::Integer(17))),
            right: Box::new(lit(PropertyValue::Integer(5))),
        };
        assert_eq!(
            eval(&expr, &EvalContext::new()).unwrap(),
            PropertyValue::Integer(2)
        );
    }

    #[test]
    fn test_list_comprehension() {
        let expr = Expression::ListComprehension {
            variable: "x".to_string(),
            list: Box::new(lit(PropertyValue::List(vec![
                PropertyValue::Integer(1),
                PropertyValue::Integer(2),
                PropertyValue::Integer(3),
                PropertyValue::Integer(4),
            ]))),
            predicate: Some(Box::new(Expression::BinOp {
                op: BinaryOp::Gt,
                left: Box::new(Expression::Variable("x".to_string())),
                right: Box::new(lit(PropertyValue::Integer(2))),
            })),
            projection: Some(Box::new(Expression::BinOp {
                op: BinaryOp::Mul,
                left: Box::new(Expression::Variable("x".to_string())),
                right: Box::new(lit(PropertyValue::Integer(10))),
            })),
        };
        let result = eval(&expr, &EvalContext::new()).unwrap();
        assert_eq!(
            result,
            PropertyValue::List(vec![PropertyValue::Integer(30), PropertyValue::Integer(40),])
        );
    }

    #[test]
    fn test_regex_match() {
        let expr = Expression::BinOp {
            op: BinaryOp::RegexMatch,
            left: Box::new(lit(PropertyValue::String("hello123".into()))),
            right: Box::new(lit(PropertyValue::String(r"^\w+\d+$".into()))),
        };
        assert_eq!(
            eval(&expr, &EvalContext::new()).unwrap(),
            PropertyValue::Boolean(true)
        );
    }

    #[test]
    fn test_and_short_circuit_on_false_left() {
        // false AND <error> should not evaluate right side
        let expr = Expression::BinOp {
            op: BinaryOp::And,
            left: Box::new(lit(PropertyValue::Boolean(false))),
            right: Box::new(Expression::Property(
                Box::new(lit(PropertyValue::Null)), // accessing property on Null would normally fail
                "invalid".to_string(),
            )),
        };
        // Should return false without evaluating right side
        assert_eq!(
            eval(&expr, &EvalContext::new()).unwrap(),
            PropertyValue::Boolean(false)
        );
    }

    #[test]
    fn test_and_evaluates_right_when_left_true() {
        let expr = Expression::BinOp {
            op: BinaryOp::And,
            left: Box::new(lit(PropertyValue::Boolean(true))),
            right: Box::new(lit(PropertyValue::Boolean(false))),
        };
        assert_eq!(
            eval(&expr, &EvalContext::new()).unwrap(),
            PropertyValue::Boolean(false)
        );
    }

    #[test]
    fn test_or_short_circuit_on_true_left() {
        // true OR <error> should not evaluate right side
        let expr = Expression::BinOp {
            op: BinaryOp::Or,
            left: Box::new(lit(PropertyValue::Boolean(true))),
            right: Box::new(Expression::Variable("nonexistent".to_string())),
        };
        // Should return true without evaluating right side
        assert_eq!(
            eval(&expr, &EvalContext::new()).unwrap(),
            PropertyValue::Boolean(true)
        );
    }

    #[test]
    fn test_or_evaluates_right_when_left_false() {
        let expr = Expression::BinOp {
            op: BinaryOp::Or,
            left: Box::new(lit(PropertyValue::Boolean(false))),
            right: Box::new(lit(PropertyValue::Boolean(true))),
        };
        assert_eq!(
            eval(&expr, &EvalContext::new()).unwrap(),
            PropertyValue::Boolean(true)
        );
    }

    #[test]
    fn test_null_safe_check_with_short_circuit() {
        // Simulates: n.prop IS NOT NULL AND n.prop > 10
        let ctx = EvalContext::new().with_variable("n_prop", PropertyValue::Null);

        let expr = Expression::BinOp {
            op: BinaryOp::And,
            left: Box::new(Expression::BinOp {
                op: BinaryOp::Ne,
                left: Box::new(var("n_prop")),
                right: Box::new(lit(PropertyValue::Null)),
            }),
            right: Box::new(Expression::BinOp {
                op: BinaryOp::Gt,
                left: Box::new(var("n_prop")),
                right: Box::new(lit(PropertyValue::Integer(10))),
            }),
        };

        // Left side evaluates to false (null == null),
        // so right side should not be evaluated (would cause type error)
        assert_eq!(eval(&expr, &ctx).unwrap(), PropertyValue::Boolean(false));
    }
}
