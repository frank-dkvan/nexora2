//! UDF type definitions — expression-based UDFs and registry types.

use nexora_id::PropertyValue;
use std::collections::HashMap;

/// A declarative expression UDF that can be serialized in recipes.
///
/// Example JSON:
/// ```json
/// {"type": "Map", "name": "risk_score", "expr": "speed * 1.5 + 10"}
/// ```
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ExpressionUdf {
    /// UDF name (used as the output key for Map UDFs).
    pub name: String,
    /// UDF kind: filter, map, or enrich.
    #[serde(rename = "type")]
    pub kind: ExpressionUdfKind,
    /// Expression string (simple arithmetic / comparison).
    pub expr: String,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ExpressionUdfKind {
    Filter,
    Map,
    Enrich,
}

/// Operators supported by the expression evaluator.
#[derive(Clone, Debug, PartialEq)]
enum Op {
    Number(f64),
    Field(String),
    Add,
    Sub,
    Mul,
    Div,
    Lt,
    Le,
    Gt,
    Ge,
    Eq,
    Ne,
    And,
    Or,
}

/// A minimal expression evaluator for UDF expressions.
/// Supports: +, -, *, /, <, <=, >, >=, ==, !=, &&, ||, and field references.
pub struct ExprEvaluator {
    tokens: Vec<Op>,
}

impl ExprEvaluator {
    /// Parse an expression string into an evaluator.
    pub fn parse(expr: &str) -> Result<Self, String> {
        let tokens = tokenize(expr)?;
        Ok(Self { tokens })
    }

    /// Evaluate the expression against a set of properties.
    pub fn eval(&self, props: &HashMap<String, PropertyValue>) -> Result<PropertyValue, String> {
        let mut pos = 0;
        let result = self.eval_expr(&mut pos, props)?;
        Ok(result)
    }

    fn eval_expr(
        &self,
        pos: &mut usize,
        props: &HashMap<String, PropertyValue>,
    ) -> Result<PropertyValue, String> {
        let left = self.eval_term(pos, props)?;
        let result = self.eval_expr_tail(left, pos, props)?;
        Ok(result)
    }

    fn eval_expr_tail(
        &self,
        mut left: PropertyValue,
        pos: &mut usize,
        props: &HashMap<String, PropertyValue>,
    ) -> Result<PropertyValue, String> {
        while *pos < self.tokens.len() {
            match &self.tokens[*pos] {
                Op::Add => {
                    *pos += 1;
                    let right = self.eval_term(pos, props)?;
                    left = arith(&left, &right, '+')?;
                }
                Op::Sub => {
                    *pos += 1;
                    let right = self.eval_term(pos, props)?;
                    left = arith(&left, &right, '-')?;
                }
                Op::Lt | Op::Le | Op::Gt | Op::Ge | Op::Eq | Op::Ne => {
                    let op = match &self.tokens[*pos] {
                        Op::Lt => '<',
                        Op::Le => 'L',
                        Op::Gt => '>',
                        Op::Ge => 'G',
                        Op::Eq => '=',
                        Op::Ne => 'N',
                        _ => unreachable!(),
                    };
                    *pos += 1;
                    let right = self.eval_term(pos, props)?;
                    left = compare(&left, &right, op)?;
                }
                Op::And => {
                    *pos += 1;
                    let right = self.eval_term(pos, props)?;
                    let lb = left.as_bool().unwrap_or(false);
                    let rb = right.as_bool().unwrap_or(false);
                    left = PropertyValue::Boolean(lb && rb);
                }
                Op::Or => {
                    *pos += 1;
                    let right = self.eval_term(pos, props)?;
                    let lb = left.as_bool().unwrap_or(false);
                    let rb = right.as_bool().unwrap_or(false);
                    left = PropertyValue::Boolean(lb || rb);
                }
                _ => break,
            }
        }
        Ok(left)
    }

    fn eval_term(
        &self,
        pos: &mut usize,
        props: &HashMap<String, PropertyValue>,
    ) -> Result<PropertyValue, String> {
        let left = self.eval_factor(pos, props)?;
        let mut result = left;
        while *pos < self.tokens.len() {
            match &self.tokens[*pos] {
                Op::Mul => {
                    *pos += 1;
                    let right = self.eval_factor(pos, props)?;
                    result = arith(&result, &right, '*')?;
                }
                Op::Div => {
                    *pos += 1;
                    let right = self.eval_factor(pos, props)?;
                    result = arith(&result, &right, '/')?;
                }
                _ => break,
            }
        }
        Ok(result)
    }

    fn eval_factor(
        &self,
        pos: &mut usize,
        props: &HashMap<String, PropertyValue>,
    ) -> Result<PropertyValue, String> {
        if *pos >= self.tokens.len() {
            return Err("unexpected end of expression".into());
        }
        match &self.tokens[*pos] {
            Op::Number(n) => {
                *pos += 1;
                Ok(PropertyValue::Float(*n))
            }
            Op::Field(name) => {
                *pos += 1;
                Ok(props.get(name).cloned().unwrap_or(PropertyValue::Null))
            }
            _ => Err(format!("unexpected token at position {}", pos)),
        }
    }
}

fn arith(a: &PropertyValue, b: &PropertyValue, op: char) -> Result<PropertyValue, String> {
    let av = a
        .as_f64()
        .ok_or_else(|| "non-numeric operand".to_string())?;
    let bv = b
        .as_f64()
        .ok_or_else(|| "non-numeric operand".to_string())?;
    let result = match op {
        '+' => av + bv,
        '-' => av - bv,
        '*' => av * bv,
        '/' => {
            if bv == 0.0 {
                return Err("division by zero".into());
            }
            av / bv
        }
        _ => return Err(format!("unknown operator: {}", op)),
    };
    Ok(PropertyValue::Float(result))
}

fn compare(a: &PropertyValue, b: &PropertyValue, op: char) -> Result<PropertyValue, String> {
    let result = match op {
        '<' => a.as_f64().unwrap_or(0.0) < b.as_f64().unwrap_or(0.0),
        'L' => a.as_f64().unwrap_or(0.0) <= b.as_f64().unwrap_or(0.0),
        '>' => a.as_f64().unwrap_or(0.0) > b.as_f64().unwrap_or(0.0),
        'G' => a.as_f64().unwrap_or(0.0) >= b.as_f64().unwrap_or(0.0),
        '=' => a == b,
        'N' => a != b,
        _ => return Err(format!("unknown comparison: {}", op)),
    };
    Ok(PropertyValue::Boolean(result))
}

fn tokenize(expr: &str) -> Result<Vec<Op>, String> {
    let mut tokens = Vec::new();
    let chars: Vec<char> = expr.chars().collect();
    let mut i = 0;

    while i < chars.len() {
        let c = chars[i];

        // Skip whitespace
        if c.is_whitespace() {
            i += 1;
            continue;
        }

        // Number
        if c.is_ascii_digit() || (c == '.' && i + 1 < chars.len() && chars[i + 1].is_ascii_digit())
        {
            let start = i;
            while i < chars.len() && (chars[i].is_ascii_digit() || chars[i] == '.') {
                i += 1;
            }
            let num: f64 = chars[start..i]
                .iter()
                .collect::<String>()
                .parse()
                .map_err(|e: std::num::ParseFloatError| e.to_string())?;
            tokens.push(Op::Number(num));
            continue;
        }

        // Field name (identifier)
        if c.is_alphabetic() || c == '_' {
            let start = i;
            while i < chars.len() && (chars[i].is_alphanumeric() || chars[i] == '_') {
                i += 1;
            }
            let name: String = chars[start..i].iter().collect();
            // Check for true/false
            match name.as_str() {
                "true" => tokens.push(Op::Number(1.0)),
                "false" => tokens.push(Op::Number(0.0)),
                _ => tokens.push(Op::Field(name)),
            }
            continue;
        }

        // Operators
        match c {
            '+' => {
                tokens.push(Op::Add);
                i += 1;
            }
            '-' => {
                tokens.push(Op::Sub);
                i += 1;
            }
            '*' => {
                tokens.push(Op::Mul);
                i += 1;
            }
            '/' => {
                tokens.push(Op::Div);
                i += 1;
            }
            '<' => {
                if i + 1 < chars.len() && chars[i + 1] == '=' {
                    tokens.push(Op::Le);
                    i += 2;
                } else {
                    tokens.push(Op::Lt);
                    i += 1;
                }
            }
            '>' => {
                if i + 1 < chars.len() && chars[i + 1] == '=' {
                    tokens.push(Op::Ge);
                    i += 2;
                } else {
                    tokens.push(Op::Gt);
                    i += 1;
                }
            }
            '=' => {
                if i + 1 < chars.len() && chars[i + 1] == '=' {
                    tokens.push(Op::Eq);
                    i += 2;
                } else {
                    tokens.push(Op::Eq);
                    i += 1;
                }
            }
            '!' => {
                if i + 1 < chars.len() && chars[i + 1] == '=' {
                    tokens.push(Op::Ne);
                    i += 2;
                } else {
                    return Err(format!("unexpected '!' at position {}", i));
                }
            }
            '&' => {
                if i + 1 < chars.len() && chars[i + 1] == '&' {
                    tokens.push(Op::And);
                    i += 2;
                } else {
                    return Err(format!("unexpected '&' at position {}", i));
                }
            }
            '|' => {
                if i + 1 < chars.len() && chars[i + 1] == '|' {
                    tokens.push(Op::Or);
                    i += 2;
                } else {
                    return Err(format!("unexpected '|' at position {}", i));
                }
            }
            _ => return Err(format!("unexpected character '{}' at position {}", c, i)),
        }
    }

    Ok(tokens)
}

/// Convert an `ExpressionUdf` into a native UDF implementing `UserDefinedFunction`.
pub fn make_expression_udf(spec: ExpressionUdf) -> Result<impl crate::UserDefinedFunction, String> {
    let evaluator = ExprEvaluator::parse(&spec.expr)?;
    let name = spec.name;
    let kind = match spec.kind {
        ExpressionUdfKind::Filter => crate::UdfKind::Filter,
        ExpressionUdfKind::Map => crate::UdfKind::Map,
        ExpressionUdfKind::Enrich => crate::UdfKind::Enrich,
    };
    Ok(ExpressionUdfImpl {
        name,
        kind,
        evaluator,
    })
}

struct ExpressionUdfImpl {
    name: String,
    kind: crate::UdfKind,
    evaluator: ExprEvaluator,
}

impl crate::UserDefinedFunction for ExpressionUdfImpl {
    fn name(&self) -> &str {
        &self.name
    }

    fn kind(&self) -> crate::UdfKind {
        self.kind.clone()
    }

    fn execute(
        &self,
        properties: &HashMap<String, PropertyValue>,
    ) -> Result<PropertyValue, crate::UdfError> {
        self.evaluator
            .eval(properties)
            .map_err(crate::UdfError::Execution)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::UserDefinedFunction;

    #[test]
    fn test_parse_and_eval_arithmetic() {
        let ev = ExprEvaluator::parse("speed * 1.5 + 10").unwrap();
        let mut props = HashMap::new();
        props.insert("speed".into(), PropertyValue::Float(80.0));
        let result = ev.eval(&props).unwrap();
        assert_eq!(result.as_f64(), Some(130.0));
    }

    #[test]
    fn test_parse_and_eval_comparison() {
        let ev = ExprEvaluator::parse("speed > 50").unwrap();
        let mut props = HashMap::new();
        props.insert("speed".into(), PropertyValue::Float(80.0));
        let result = ev.eval(&props).unwrap();
        assert_eq!(result, PropertyValue::Boolean(true));
    }

    #[test]
    fn test_expression_udf_filter() {
        let spec = ExpressionUdf {
            name: "high_speed".into(),
            kind: ExpressionUdfKind::Filter,
            expr: "speed > 50".into(),
        };
        let udf = make_expression_udf(spec).unwrap();
        let mut props = HashMap::new();
        props.insert("speed".into(), PropertyValue::Float(80.0));
        let result = udf.execute(&props).unwrap();
        assert_eq!(result, PropertyValue::Boolean(true));
    }

    #[test]
    fn test_expression_udf_map() {
        let spec = ExpressionUdf {
            name: "risk_score".into(),
            kind: ExpressionUdfKind::Map,
            expr: "speed * 2 + 5".into(),
        };
        let udf = make_expression_udf(spec).unwrap();
        let mut props = HashMap::new();
        props.insert("speed".into(), PropertyValue::Float(40.0));
        let result = udf.execute(&props).unwrap();
        assert_eq!(result.as_f64(), Some(85.0));
    }

    #[test]
    fn test_expression_serde() {
        let spec = ExpressionUdf {
            name: "test".into(),
            kind: ExpressionUdfKind::Filter,
            expr: "x > 10".into(),
        };
        let json = serde_json::to_string(&spec).unwrap();
        let parsed: ExpressionUdf = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.name, "test");
        assert_eq!(parsed.kind, ExpressionUdfKind::Filter);
    }
}
