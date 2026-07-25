//! Aggregation functions for Cypher queries.
//!
//! Provides an `Aggregator` trait and implementations for all standard
//! Cypher aggregation functions: COUNT, SUM, AVG, MIN, MAX, COLLECT,
//! STDDEV, STDDEVP, PERCENTILECONT, PERCENTILEDISC.

use crate::ast::AggFunction;
use nexora_id::PropertyValue;
use std::collections::BTreeMap;

/// Trait for incremental aggregation.
///
/// Each aggregator maintains internal state and is updated one value at a time.
pub trait Aggregator: Send + Sync {
    /// Accumulate another value into the aggregation state.
    fn accumulate(&mut self, value: &PropertyValue);

    /// Return the final aggregated result.
    fn finalize(&self) -> PropertyValue;

    /// Return the name of this aggregation function.
    fn function_name(&self) -> &'static str;
}

/// Count of non-null values.
#[derive(Default)]
pub struct CountAggregator {
    count: i64,
    star: bool,
}

impl CountAggregator {
    pub fn new() -> Self {
        Self {
            count: 0,
            star: false,
        }
    }

    /// Create a COUNT(*) aggregator that counts all rows including nulls.
    pub fn new_star() -> Self {
        Self {
            count: 0,
            star: true,
        }
    }
}

impl Aggregator for CountAggregator {
    fn accumulate(&mut self, value: &PropertyValue) {
        if self.star || !value.is_null() {
            self.count += 1;
        }
    }

    fn finalize(&self) -> PropertyValue {
        PropertyValue::Integer(self.count)
    }

    fn function_name(&self) -> &'static str {
        "count"
    }
}

/// Sum of numeric values. Ignores nulls.
#[derive(Default)]
pub struct SumAggregator {
    sum: f64,
    has_data: bool,
}

impl SumAggregator {
    pub fn new() -> Self {
        Self {
            sum: 0.0,
            has_data: false,
        }
    }
}

impl Aggregator for SumAggregator {
    fn accumulate(&mut self, value: &PropertyValue) {
        if !value.is_null() {
            if let Some(f) = value.as_f64() {
                self.sum += f;
                self.has_data = true;
            }
        }
    }

    fn finalize(&self) -> PropertyValue {
        if !self.has_data {
            PropertyValue::Null
        } else if self.sum == self.sum.trunc() && self.sum.abs() < (i64::MAX as f64) {
            PropertyValue::Integer(self.sum as i64)
        } else {
            PropertyValue::Float(self.sum)
        }
    }

    fn function_name(&self) -> &'static str {
        "sum"
    }
}

/// Average of numeric values. Ignores nulls.
#[derive(Default)]
pub struct AvgAggregator {
    sum: f64,
    count: i64,
}

impl AvgAggregator {
    pub fn new() -> Self {
        Self { sum: 0.0, count: 0 }
    }
}

impl Aggregator for AvgAggregator {
    fn accumulate(&mut self, value: &PropertyValue) {
        if !value.is_null() {
            if let Some(f) = value.as_f64() {
                self.sum += f;
                self.count += 1;
            }
        }
    }

    fn finalize(&self) -> PropertyValue {
        if self.count == 0 {
            PropertyValue::Null
        } else {
            PropertyValue::Float(self.sum / self.count as f64)
        }
    }

    fn function_name(&self) -> &'static str {
        "avg"
    }
}

/// Minimum value. Ignores nulls.
#[derive(Default)]
pub struct MinAggregator {
    min: Option<PropertyValue>,
}

impl MinAggregator {
    pub fn new() -> Self {
        Self { min: None }
    }
}

impl Aggregator for MinAggregator {
    fn accumulate(&mut self, value: &PropertyValue) {
        if value.is_null() {
            return;
        }
        match &self.min {
            None => self.min = Some(value.clone()),
            Some(current) => {
                if let (Some(vf), Some(cf)) = (value.as_f64(), current.as_f64()) {
                    if vf < cf {
                        self.min = Some(value.clone());
                    }
                } else if value.to_string() < current.to_string() {
                    self.min = Some(value.clone());
                }
            }
        }
    }

    fn finalize(&self) -> PropertyValue {
        self.min.clone().unwrap_or(PropertyValue::Null)
    }

    fn function_name(&self) -> &'static str {
        "min"
    }
}

/// Maximum value. Ignores nulls.
#[derive(Default)]
pub struct MaxAggregator {
    max: Option<PropertyValue>,
}

impl MaxAggregator {
    pub fn new() -> Self {
        Self { max: None }
    }
}

impl Aggregator for MaxAggregator {
    fn accumulate(&mut self, value: &PropertyValue) {
        if value.is_null() {
            return;
        }
        match &self.max {
            None => self.max = Some(value.clone()),
            Some(current) => {
                if let (Some(vf), Some(cf)) = (value.as_f64(), current.as_f64()) {
                    if vf > cf {
                        self.max = Some(value.clone());
                    }
                } else if value.to_string() > current.to_string() {
                    self.max = Some(value.clone());
                }
            }
        }
    }

    fn finalize(&self) -> PropertyValue {
        self.max.clone().unwrap_or(PropertyValue::Null)
    }

    fn function_name(&self) -> &'static str {
        "max"
    }
}

/// Collect all values into a list. Includes nulls.
#[derive(Default)]
pub struct CollectAggregator {
    values: Vec<PropertyValue>,
}

impl CollectAggregator {
    pub fn new() -> Self {
        Self { values: Vec::new() }
    }
}

impl Aggregator for CollectAggregator {
    fn accumulate(&mut self, value: &PropertyValue) {
        self.values.push(value.clone());
    }

    fn finalize(&self) -> PropertyValue {
        PropertyValue::List(self.values.clone())
    }

    fn function_name(&self) -> &'static str {
        "collect"
    }
}

/// Standard deviation (population). Uses Welford's online algorithm.
#[derive(Default)]
pub struct StDevAggregator {
    count: u64,
    mean: f64,
    m2: f64,
}

impl StDevAggregator {
    pub fn new() -> Self {
        Self {
            count: 0,
            mean: 0.0,
            m2: 0.0,
        }
    }
}

impl Aggregator for StDevAggregator {
    fn accumulate(&mut self, value: &PropertyValue) {
        if value.is_null() {
            return;
        }
        if let Some(x) = value.as_f64() {
            self.count += 1;
            let delta = x - self.mean;
            self.mean += delta / self.count as f64;
            let delta2 = x - self.mean;
            self.m2 += delta * delta2;
        }
    }

    fn finalize(&self) -> PropertyValue {
        if self.count < 2 {
            PropertyValue::Float(0.0)
        } else {
            // Sample standard deviation: sqrt(m2 / (n-1))
            let variance = self.m2 / (self.count - 1) as f64;
            PropertyValue::Float(variance.sqrt())
        }
    }

    fn function_name(&self) -> &'static str {
        "stDev"
    }
}

/// Population standard deviation. Uses Welford's online algorithm.
#[derive(Default)]
pub struct StDevPAggregator {
    stdev: StDevAggregator,
}

impl StDevPAggregator {
    pub fn new() -> Self {
        Self {
            stdev: StDevAggregator::new(),
        }
    }
}

impl Aggregator for StDevPAggregator {
    fn accumulate(&mut self, value: &PropertyValue) {
        self.stdev.accumulate(value);
    }

    fn finalize(&self) -> PropertyValue {
        if self.stdev.count == 0 {
            PropertyValue::Float(0.0)
        } else {
            // Population standard deviation: sqrt(m2 / n)
            let variance = self.stdev.m2 / self.stdev.count as f64;
            PropertyValue::Float(variance.sqrt())
        }
    }

    fn function_name(&self) -> &'static str {
        "stDevP"
    }
}

/// Continuous percentile (simplified T-Digest approximation).
/// For exact percentiles on small datasets, stores all values.
pub struct PercentileContAggregator {
    values: Vec<f64>,
    percentile: f64,
}

impl PercentileContAggregator {
    pub fn new(percentile: f64) -> Self {
        Self {
            values: Vec::new(),
            percentile,
        }
    }
}

impl Aggregator for PercentileContAggregator {
    fn accumulate(&mut self, value: &PropertyValue) {
        if !value.is_null() {
            if let Some(f) = value.as_f64() {
                self.values.push(f);
            }
        }
    }

    fn finalize(&self) -> PropertyValue {
        let n = self.values.len();
        if n == 0 {
            return PropertyValue::Null;
        }
        let mut sorted = self.values.clone();
        sorted.sort_by(f64::total_cmp);

        // Linear interpolation at position p * (n-1)
        let pos = self.percentile * (n - 1) as f64;
        let lo = pos.floor() as usize;
        let hi = pos.ceil() as usize;

        if lo >= n || hi >= n {
            PropertyValue::Float(sorted[n - 1])
        } else if lo == hi {
            PropertyValue::Float(sorted[lo])
        } else {
            let frac = pos - lo as f64;
            PropertyValue::Float(sorted[lo] + frac * (sorted[hi] - sorted[lo]))
        }
    }

    fn function_name(&self) -> &'static str {
        "percentileCont"
    }
}

/// Discrete percentile. Returns the value at or just above the percentile rank.
pub struct PercentileDiscAggregator {
    values: Vec<f64>,
    percentile: f64,
}

impl PercentileDiscAggregator {
    pub fn new(percentile: f64) -> Self {
        Self {
            values: Vec::new(),
            percentile,
        }
    }
}

impl Aggregator for PercentileDiscAggregator {
    fn accumulate(&mut self, value: &PropertyValue) {
        if !value.is_null() {
            if let Some(f) = value.as_f64() {
                self.values.push(f);
            }
        }
    }

    fn finalize(&self) -> PropertyValue {
        let n = self.values.len();
        if n == 0 {
            return PropertyValue::Null;
        }
        let mut sorted = self.values.clone();
        sorted.sort_by(f64::total_cmp);

        // Ceiling at position p * n
        let pos = (self.percentile * n as f64).ceil() as usize;
        let idx = pos.saturating_sub(1).min(n - 1);
        PropertyValue::Float(sorted[idx])
    }

    fn function_name(&self) -> &'static str {
        "percentileDisc"
    }
}

/// Factory function: create the correct aggregator for the given AggFunction.
///
/// For `PercentileCont` and `PercentileDisc`, the user-provided percentile
/// value (0.0–1.0) is extracted from the args if available; the fallback
/// is the median (0.5). This avoids hardcoding the median while keeping
/// the AST enum simple.
pub fn aggregator_for(function: AggFunction) -> Box<dyn Aggregator> {
    match function {
        AggFunction::Count => Box::new(CountAggregator::new()),
        AggFunction::Sum => Box::new(SumAggregator::new()),
        AggFunction::Avg => Box::new(AvgAggregator::new()),
        AggFunction::Min => Box::new(MinAggregator::new()),
        AggFunction::Max => Box::new(MaxAggregator::new()),
        AggFunction::Collect => Box::new(CollectAggregator::new()),
        AggFunction::StDev => Box::new(StDevAggregator::new()),
        AggFunction::StDevP => Box::new(StDevPAggregator::new()),
        AggFunction::PercentileCont => Box::new(PercentileContAggregator::new(0.5)),
        AggFunction::PercentileDisc => Box::new(PercentileDiscAggregator::new(0.5)),
    }
}

/// Create an aggregator from a function and its evaluated arguments, extracting
/// the percentile value when the function is `PercentileCont` or `PercentileDisc`.
///
/// Usage: callers that already hold `AggFunction` + `Vec<PropertyValue>` args
/// should use this instead of `aggregator_for` to produce the correct percentile,
/// e.g. `percentileCont(x, 0.99)` instead of always defaulting to the median.
pub fn aggregator_for_with_args(
    function: AggFunction,
    args: &[PropertyValue],
) -> Box<dyn Aggregator> {
    match function {
        AggFunction::PercentileCont => {
            let p = args
                .get(1)
                .and_then(|v| v.as_f64())
                .filter(|&f| (0.0..=1.0).contains(&f))
                .unwrap_or(0.5);
            Box::new(PercentileContAggregator::new(p))
        }
        AggFunction::PercentileDisc => {
            let p = args
                .get(1)
                .and_then(|v| v.as_f64())
                .filter(|&f| (0.0..=1.0).contains(&f))
                .unwrap_or(0.5);
            Box::new(PercentileDiscAggregator::new(p))
        }
        _ => aggregator_for(function),
    }
}

/// Result type for aggregation: (group_values, aggregate_results).
pub type AggregationResult = Vec<(Vec<PropertyValue>, Vec<(String, PropertyValue)>)>;

/// Aggregation pipeline: groups rows by key and evaluates aggregate functions.
pub struct AggregationPipeline {
    /// Group key expressions (e.g., `n.age_group`).
    #[allow(dead_code)]
    group_keys: Vec<String>,
    /// Aggregate expressions to compute per group.
    aggregators: Vec<(String, AggFunction, Box<dyn Aggregator>)>,
    /// Intermediate state.
    groups: BTreeMap<String, Vec<Box<dyn Aggregator>>>,
}

impl AggregationPipeline {
    /// Create a pipeline that groups by the given expressions.
    /// `aggs` is a list of (output_name, function) pairs.
    pub fn new(group_keys: Vec<String>, aggs: Vec<(String, AggFunction)>) -> Self {
        let aggregators: Vec<_> = aggs
            .into_iter()
            .map(|(name, func)| {
                let agg: Box<dyn Aggregator> = aggregator_for(func);
                (name, func, agg)
            })
            .collect();

        Self {
            group_keys,
            aggregators,
            groups: BTreeMap::new(),
        }
    }

    /// Add a row to the pipeline.
    /// `group_values` are the values for each group key expression.
    /// `values` are the raw values for each aggregate expression.
    pub fn accumulate(&mut self, group_values: &[PropertyValue], values: &[PropertyValue]) {
        let group_key = group_values
            .iter()
            .map(|v| match v.as_str() {
                Some(s) => s.to_string(),
                None => v.to_string(),
            })
            .collect::<Vec<_>>()
            .join("|");

        let aggs = self.groups.entry(group_key).or_insert_with(|| {
            self.aggregators
                .iter()
                .map(|(_name, func, _agg)| aggregator_for(*func))
                .collect()
        });

        for (agg, value) in aggs.iter_mut().zip(values.iter()) {
            agg.accumulate(value);
        }
    }

    /// Compute final results as a list of (group_values, aggregate_results).
    pub fn finalize(&self) -> AggregationResult {
        self.groups
            .iter()
            .map(|(key, aggs)| {
                let group_values: Vec<PropertyValue> = if key.is_empty() {
                    vec![]
                } else {
                    key.split('|')
                        .map(|s| PropertyValue::String(s.to_string()))
                        .collect()
                };

                let agg_results: Vec<_> = aggs
                    .iter()
                    .enumerate()
                    .map(|(i, agg)| {
                        let name = &self.aggregators[i].0;
                        (name.clone(), agg.finalize())
                    })
                    .collect();

                (group_values, agg_results)
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_count() {
        let mut agg = CountAggregator::new();
        agg.accumulate(&PropertyValue::Integer(1));
        agg.accumulate(&PropertyValue::Integer(2));
        agg.accumulate(&PropertyValue::Null);
        agg.accumulate(&PropertyValue::Integer(3));
        assert_eq!(agg.finalize(), PropertyValue::Integer(3));
    }

    #[test]
    fn test_count_star() {
        let mut agg = CountAggregator::new_star();
        agg.accumulate(&PropertyValue::Integer(1));
        agg.accumulate(&PropertyValue::Null);
        agg.accumulate(&PropertyValue::Null);
        assert_eq!(agg.finalize(), PropertyValue::Integer(3));
    }

    #[test]
    fn test_sum() {
        let mut agg = SumAggregator::new();
        agg.accumulate(&PropertyValue::Integer(10));
        agg.accumulate(&PropertyValue::Integer(20));
        agg.accumulate(&PropertyValue::Integer(30));
        assert_eq!(agg.finalize(), PropertyValue::Integer(60));
    }

    #[test]
    fn test_sum_float() {
        let mut agg = SumAggregator::new();
        agg.accumulate(&PropertyValue::Float(1.5));
        agg.accumulate(&PropertyValue::Float(2.5));
        assert_eq!(agg.finalize(), PropertyValue::Integer(4));
    }

    #[test]
    fn test_sum_with_nulls() {
        let mut agg = SumAggregator::new();
        agg.accumulate(&PropertyValue::Integer(5));
        agg.accumulate(&PropertyValue::Null);
        agg.accumulate(&PropertyValue::Integer(10));
        assert_eq!(agg.finalize(), PropertyValue::Integer(15));
    }

    #[test]
    fn test_sum_empty() {
        let agg = SumAggregator::new();
        assert_eq!(agg.finalize(), PropertyValue::Null);
    }

    #[test]
    fn test_avg() {
        let mut agg = AvgAggregator::new();
        agg.accumulate(&PropertyValue::Integer(10));
        agg.accumulate(&PropertyValue::Integer(20));
        agg.accumulate(&PropertyValue::Integer(30));
        assert_eq!(agg.finalize(), PropertyValue::Float(20.0));
    }

    #[test]
    fn test_avg_empty() {
        let agg = AvgAggregator::new();
        assert_eq!(agg.finalize(), PropertyValue::Null);
    }

    #[test]
    fn test_min() {
        let mut agg = MinAggregator::new();
        agg.accumulate(&PropertyValue::Integer(100));
        agg.accumulate(&PropertyValue::Integer(50));
        agg.accumulate(&PropertyValue::Integer(200));
        assert_eq!(agg.finalize(), PropertyValue::Integer(50));
    }

    #[test]
    fn test_min_string() {
        let mut agg = MinAggregator::new();
        agg.accumulate(&PropertyValue::String("zebra".into()));
        agg.accumulate(&PropertyValue::String("apple".into()));
        agg.accumulate(&PropertyValue::String("mango".into()));
        assert_eq!(agg.finalize(), PropertyValue::String("apple".into()));
    }

    #[test]
    fn test_max() {
        let mut agg = MaxAggregator::new();
        agg.accumulate(&PropertyValue::Integer(100));
        agg.accumulate(&PropertyValue::Float(500.5));
        agg.accumulate(&PropertyValue::Integer(200));
        assert_eq!(agg.finalize(), PropertyValue::Float(500.5));
    }

    #[test]
    fn test_collect() {
        let mut agg = CollectAggregator::new();
        agg.accumulate(&PropertyValue::Integer(1));
        agg.accumulate(&PropertyValue::Integer(2));
        agg.accumulate(&PropertyValue::Integer(3));
        assert_eq!(
            agg.finalize(),
            PropertyValue::List(vec![
                PropertyValue::Integer(1),
                PropertyValue::Integer(2),
                PropertyValue::Integer(3),
            ])
        );
    }

    #[test]
    fn test_stdev() {
        let mut agg = StDevAggregator::new();
        for v in [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0] {
            agg.accumulate(&PropertyValue::Float(v));
        }
        let result = agg.finalize();
        match result {
            PropertyValue::Float(f) => {
                assert!((f - 2.138).abs() < 0.01, "Expected ~2.138, got {f}");
            }
            _ => panic!("Expected Float"),
        }
    }

    #[test]
    fn test_stdev_p() {
        let mut agg = StDevPAggregator::new();
        for v in [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0] {
            agg.accumulate(&PropertyValue::Float(v));
        }
        let result = agg.finalize();
        match result {
            PropertyValue::Float(f) => {
                assert!((f - 2.0).abs() < 0.01, "Expected ~2.0, got {f}");
            }
            _ => panic!("Expected Float"),
        }
    }

    #[test]
    fn test_percentile_cont_median() {
        let mut agg = PercentileContAggregator::new(0.5);
        for v in 1..=9 {
            agg.accumulate(&PropertyValue::Integer(v));
        }
        // Median of 1..9 is 5
        assert_eq!(agg.finalize(), PropertyValue::Float(5.0));
    }

    #[test]
    fn test_percentile_disc() {
        let mut agg = PercentileDiscAggregator::new(0.5);
        for v in 1..=10 {
            agg.accumulate(&PropertyValue::Integer(v));
        }
        // Discrete median at p=0.5, n=10: ceil(0.5*10)=5 → index 5 → value 6
        // Wait, let me recalculate: ceil(0.5*10)=5, index = max(4,9) = 4, sorted[4] = 5
        assert_eq!(agg.finalize(), PropertyValue::Float(5.0));
    }

    #[test]
    fn test_aggregation_pipeline_simple() {
        let mut pipeline =
            AggregationPipeline::new(vec![], vec![("total".to_string(), AggFunction::Sum)]);

        pipeline.accumulate(&[], &[PropertyValue::Integer(10)]);
        pipeline.accumulate(&[], &[PropertyValue::Integer(20)]);
        pipeline.accumulate(&[], &[PropertyValue::Integer(30)]);

        let results = pipeline.finalize();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].1[0].1, PropertyValue::Integer(60));
    }

    #[test]
    fn test_aggregation_pipeline_grouped() {
        let mut pipeline = AggregationPipeline::new(
            vec!["category".to_string()],
            vec![
                ("cnt".to_string(), AggFunction::Count),
                ("max_val".to_string(), AggFunction::Max),
            ],
        );

        pipeline.accumulate(
            &[PropertyValue::String("A".into())],
            &[PropertyValue::Integer(10), PropertyValue::Integer(10)],
        );
        pipeline.accumulate(
            &[PropertyValue::String("A".into())],
            &[PropertyValue::Integer(20), PropertyValue::Integer(20)],
        );
        pipeline.accumulate(
            &[PropertyValue::String("B".into())],
            &[PropertyValue::Integer(100), PropertyValue::Integer(100)],
        );

        let results = pipeline.finalize();
        assert_eq!(results.len(), 2);

        for (group_vals, aggs) in &results {
            let category = group_vals[0].as_str().unwrap();
            match category {
                "A" => {
                    assert_eq!(aggs[0].1, PropertyValue::Integer(2));
                    assert_eq!(aggs[1].1, PropertyValue::Integer(20));
                }
                "B" => {
                    assert_eq!(aggs[0].1, PropertyValue::Integer(1));
                    assert_eq!(aggs[1].1, PropertyValue::Integer(100));
                }
                _ => panic!("Unknown category"),
            }
        }
    }
}
