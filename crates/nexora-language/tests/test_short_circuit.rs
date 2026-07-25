//! Tests for short-circuit evaluation in AND/OR operators (P0-3 fix verification).

use nexora_id::PropertyValue;
use nexora_language::ast::BinaryOp;
use nexora_language::evaluator::ExpressionEvaluator;
use nexora_language::{EvalContext, Expression};

#[test]
fn test_and_short_circuit_false_left() {
    // WHERE false AND n.nonexistent should not evaluate the right side
    let left = Expression::Literal(PropertyValue::Boolean(false));
    // Property access that would fail if n doesn't exist
    let right = Expression::Property(
        Box::new(Expression::Variable("n".into())),
        "nonexistent".into(),
    );

    let expr = Expression::BinOp {
        op: BinaryOp::And,
        left: Box::new(left),
        right: Box::new(right),
    };

    let ctx = EvalContext::default();
    let result = ExpressionEvaluator::evaluate(&expr, &ctx);

    // Should return false without evaluating the right side
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), PropertyValue::Boolean(false));
}

#[test]
fn test_and_short_circuit_null_safe() {
    // WHERE n.prop IS NOT NULL AND n.prop > 10
    // This is the main use case: protect against null access
    let ctx = EvalContext::default();

    // Simulate: null AND anything -> false (without evaluating right)
    let left = Expression::Literal(PropertyValue::Null);
    let right = Expression::Literal(PropertyValue::Integer(42)); // Would fail comparison if evaluated

    let expr = Expression::BinOp {
        op: BinaryOp::And,
        left: Box::new(left),
        right: Box::new(right),
    };

    let result = ExpressionEvaluator::evaluate(&expr, &ctx);
    assert!(result.is_ok());
    // null is falsy, so AND short-circuits to false
    assert_eq!(result.unwrap(), PropertyValue::Boolean(false));
}

#[test]
fn test_or_short_circuit_true_left() {
    // WHERE true OR n.nonexistent should not evaluate the right side
    let left = Expression::Literal(PropertyValue::Boolean(true));
    let right = Expression::Property(
        Box::new(Expression::Variable("n".into())),
        "nonexistent".into(),
    );

    let expr = Expression::BinOp {
        op: BinaryOp::Or,
        left: Box::new(left),
        right: Box::new(right),
    };

    let ctx = EvalContext::default();
    let result = ExpressionEvaluator::evaluate(&expr, &ctx);

    assert!(result.is_ok());
    assert_eq!(result.unwrap(), PropertyValue::Boolean(true));
}

#[test]
fn test_or_evaluates_right_when_left_false() {
    // WHERE false OR true should evaluate both sides
    let left = Expression::Literal(PropertyValue::Boolean(false));
    let right = Expression::Literal(PropertyValue::Boolean(true));

    let expr = Expression::BinOp {
        op: BinaryOp::Or,
        left: Box::new(left),
        right: Box::new(right),
    };

    let ctx = EvalContext::default();
    let result = ExpressionEvaluator::evaluate(&expr, &ctx);

    assert!(result.is_ok());
    assert_eq!(result.unwrap(), PropertyValue::Boolean(true));
}

#[test]
fn test_and_evaluates_right_when_left_true() {
    // WHERE true AND false should evaluate both sides
    let left = Expression::Literal(PropertyValue::Boolean(true));
    let right = Expression::Literal(PropertyValue::Boolean(false));

    let expr = Expression::BinOp {
        op: BinaryOp::And,
        left: Box::new(left),
        right: Box::new(right),
    };

    let ctx = EvalContext::default();
    let result = ExpressionEvaluator::evaluate(&expr, &ctx);

    assert!(result.is_ok());
    assert_eq!(result.unwrap(), PropertyValue::Boolean(false));
}

#[test]
fn test_nested_short_circuit() {
    // WHERE (false AND n.expensive1) OR (true OR n.expensive2)
    // Should only evaluate: false, true (skip both expensive calls)

    let inner_and = Expression::BinOp {
        op: BinaryOp::And,
        left: Box::new(Expression::Literal(PropertyValue::Boolean(false))),
        right: Box::new(Expression::Property(
            Box::new(Expression::Variable("n".into())),
            "expensive1".into(),
        )),
    };

    let inner_or = Expression::BinOp {
        op: BinaryOp::Or,
        left: Box::new(Expression::Literal(PropertyValue::Boolean(true))),
        right: Box::new(Expression::Property(
            Box::new(Expression::Variable("n".into())),
            "expensive2".into(),
        )),
    };

    let outer_or = Expression::BinOp {
        op: BinaryOp::Or,
        left: Box::new(inner_and),
        right: Box::new(inner_or),
    };

    let ctx = EvalContext::default();
    let result = ExpressionEvaluator::evaluate(&outer_or, &ctx);

    assert!(result.is_ok());
    assert_eq!(result.unwrap(), PropertyValue::Boolean(true));
}
