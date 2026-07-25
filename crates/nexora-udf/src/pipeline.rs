//! Extended pipeline — aggregate UDFs and pipeline composition.

use crate::{UdfError, UdfKind, UserDefinedFunction};
use nexora_id::PropertyValue;
use std::collections::HashMap;
use std::sync::Arc;

/// Stateful aggregator that accumulates values across events.
pub trait AggregateUdf: Send + Sync {
    /// Name of this aggregator (used as the output key).
    fn name(&self) -> &str;

    /// Called for each event that passes the filter stage.
    fn accumulate(&mut self, properties: &HashMap<String, PropertyValue>) -> Result<(), UdfError>;

    /// Produce the final aggregated value.
    fn finalize(&self) -> PropertyValue;

    /// Reset the aggregator to its initial state (for tumbling windows).
    fn reset(&mut self);
}

/// Built-in aggregators.
pub enum BuiltinAggregate {
    /// Running count of events.
    Count { count: u64 },
    /// Running sum of a numeric field.
    Sum { field: String, sum: f64 },
    /// Running average of a numeric field.
    Avg { field: String, sum: f64, count: u64 },
    /// Running minimum of a numeric field.
    Min { field: String, min: Option<f64> },
    /// Running maximum of a numeric field.
    Max { field: String, max: Option<f64> },
}

impl BuiltinAggregate {
    pub fn count() -> Self {
        Self::Count { count: 0 }
    }
    pub fn sum(field: &str) -> Self {
        Self::Sum {
            field: field.to_string(),
            sum: 0.0,
        }
    }
    pub fn avg(field: &str) -> Self {
        Self::Avg {
            field: field.to_string(),
            sum: 0.0,
            count: 0,
        }
    }
    pub fn min(field: &str) -> Self {
        Self::Min {
            field: field.to_string(),
            min: None,
        }
    }
    pub fn max(field: &str) -> Self {
        Self::Max {
            field: field.to_string(),
            max: None,
        }
    }
}

impl AggregateUdf for BuiltinAggregate {
    fn name(&self) -> &str {
        match self {
            Self::Count { .. } => "count",
            Self::Sum { .. } => "sum",
            Self::Avg { .. } => "avg",
            Self::Min { .. } => "min",
            Self::Max { .. } => "max",
        }
    }

    fn accumulate(&mut self, props: &HashMap<String, PropertyValue>) -> Result<(), UdfError> {
        match self {
            Self::Count { count } => {
                *count += 1;
            }
            Self::Sum { field, sum } => {
                if let Some(v) = props.get(field).and_then(|p| p.as_f64()) {
                    *sum += v;
                }
            }
            Self::Avg { field, sum, count } => {
                if let Some(v) = props.get(field).and_then(|p| p.as_f64()) {
                    *sum += v;
                    *count += 1;
                }
            }
            Self::Min { field, min } => {
                if let Some(v) = props.get(field).and_then(|p| p.as_f64()) {
                    *min = Some(min.map_or(v, |m| m.min(v)));
                }
            }
            Self::Max { field, max } => {
                if let Some(v) = props.get(field).and_then(|p| p.as_f64()) {
                    *max = Some(max.map_or(v, |m| m.max(v)));
                }
            }
        }
        Ok(())
    }

    fn finalize(&self) -> PropertyValue {
        match self {
            Self::Count { count } => PropertyValue::Integer(*count as i64),
            Self::Sum { sum, .. } => PropertyValue::Float(*sum),
            Self::Avg { sum, count, .. } => {
                if *count == 0 {
                    PropertyValue::Float(0.0)
                } else {
                    PropertyValue::Float(*sum / *count as f64)
                }
            }
            Self::Min { min, .. } => PropertyValue::Float(min.unwrap_or(0.0)),
            Self::Max { max, .. } => PropertyValue::Float(max.unwrap_or(0.0)),
        }
    }

    fn reset(&mut self) {
        match self {
            Self::Count { count } => *count = 0,
            Self::Sum { sum, .. } => *sum = 0.0,
            Self::Avg { sum, count, .. } => {
                *sum = 0.0;
                *count = 0;
            }
            Self::Min { min, .. } => *min = None,
            Self::Max { max, .. } => *max = None,
        }
    }
}

/// Extended pipeline with filter → map → enrich → aggregate stages.
pub struct AggregatePipeline {
    filters: Vec<Arc<dyn UserDefinedFunction>>,
    maps: Vec<Arc<dyn UserDefinedFunction>>,
    enrichers: Vec<Arc<dyn UserDefinedFunction>>,
    aggregators: Vec<Box<dyn AggregateUdf>>,
}

impl AggregatePipeline {
    pub fn new() -> Self {
        Self {
            filters: Vec::new(),
            maps: Vec::new(),
            enrichers: Vec::new(),
            aggregators: Vec::new(),
        }
    }

    pub fn add_filter(&mut self, udf: Arc<dyn UserDefinedFunction>) -> &mut Self {
        debug_assert_eq!(udf.kind(), UdfKind::Filter);
        self.filters.push(udf);
        self
    }

    pub fn add_map(&mut self, udf: Arc<dyn UserDefinedFunction>) -> &mut Self {
        debug_assert_eq!(udf.kind(), UdfKind::Map);
        self.maps.push(udf);
        self
    }

    pub fn add_enricher(&mut self, udf: Arc<dyn UserDefinedFunction>) -> &mut Self {
        debug_assert_eq!(udf.kind(), UdfKind::Enrich);
        self.enrichers.push(udf);
        self
    }

    pub fn add_aggregator(&mut self, agg: Box<dyn AggregateUdf>) -> &mut Self {
        self.aggregators.push(agg);
        self
    }

    /// Process a single event through filter → map → enrich, then feed to aggregators.
    /// Returns `Ok(Some(props))` if the event passes all filters, `Ok(None)` if filtered out.
    pub fn process(
        &mut self,
        mut properties: HashMap<String, PropertyValue>,
    ) -> Result<Option<HashMap<String, PropertyValue>>, UdfError> {
        // 1. Filters
        for filter in &self.filters {
            match filter.execute(&properties)? {
                PropertyValue::Boolean(true) => {}
                PropertyValue::Boolean(false) => return Ok(None),
                other => {
                    return Err(UdfError::TypeMismatch {
                        expected: "Boolean".into(),
                        actual: format!("{:?}", other),
                    })
                }
            }
        }

        // 2. Maps
        for map in &self.maps {
            let result = map.execute(&properties)?;
            properties.insert(map.name().to_string(), result);
        }

        // 3. Enrichers
        for enricher in &self.enrichers {
            let result = enricher.execute(&properties)?;
            if let PropertyValue::Map(additions) = result {
                for (k, v) in additions {
                    properties.insert(k, v);
                }
            }
        }

        // 4. Feed to aggregators
        for agg in &mut self.aggregators {
            agg.accumulate(&properties)?;
        }

        Ok(Some(properties))
    }

    /// Finalize all aggregators and return results as a property map.
    pub fn finalize(&self) -> HashMap<String, PropertyValue> {
        self.aggregators
            .iter()
            .map(|agg| (agg.name().to_string(), agg.finalize()))
            .collect()
    }

    /// Reset all aggregators (e.g., for tumbling windows).
    pub fn reset(&mut self) {
        for agg in &mut self.aggregators {
            agg.reset();
        }
    }

    /// Total number of stages.
    pub fn len(&self) -> usize {
        self.filters.len() + self.maps.len() + self.enrichers.len() + self.aggregators.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Default for AggregatePipeline {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::UdfKind;

    struct SpeedFilter {
        threshold: f64,
    }
    impl UserDefinedFunction for SpeedFilter {
        fn name(&self) -> &str {
            "speed_filter"
        }
        fn kind(&self) -> UdfKind {
            UdfKind::Filter
        }
        fn execute(
            &self,
            props: &HashMap<String, PropertyValue>,
        ) -> Result<PropertyValue, UdfError> {
            let speed = props.get("speed").and_then(|v| v.as_f64()).unwrap_or(0.0);
            Ok(PropertyValue::Boolean(speed > self.threshold))
        }
    }

    #[test]
    fn test_aggregate_count() {
        let mut pipe = AggregatePipeline::new();
        pipe.add_aggregator(Box::new(BuiltinAggregate::count()));

        for _ in 0..5 {
            pipe.process(HashMap::new()).unwrap();
        }

        let results = pipe.finalize();
        assert_eq!(results.get("count"), Some(&PropertyValue::Integer(5)));
    }

    #[test]
    fn test_aggregate_sum_with_filter() {
        let mut pipe = AggregatePipeline::new();
        pipe.add_filter(Arc::new(SpeedFilter { threshold: 50.0 }));
        pipe.add_aggregator(Box::new(BuiltinAggregate::sum("speed")));

        let speeds = [30.0, 80.0, 40.0, 90.0, 100.0];
        for s in speeds {
            let mut props = HashMap::new();
            props.insert("speed".into(), PropertyValue::Float(s));
            pipe.process(props).unwrap();
        }

        let results = pipe.finalize();
        // Only 80 + 90 + 100 = 270 pass the filter
        assert_eq!(results.get("sum"), Some(&PropertyValue::Float(270.0)));
    }

    #[test]
    fn test_aggregate_avg() {
        let mut pipe = AggregatePipeline::new();
        pipe.add_aggregator(Box::new(BuiltinAggregate::avg("speed")));

        for s in [10.0, 20.0, 30.0] {
            let mut props = HashMap::new();
            props.insert("speed".into(), PropertyValue::Float(s));
            pipe.process(props).unwrap();
        }

        let results = pipe.finalize();
        let avg = results.get("avg").and_then(|v| v.as_f64()).unwrap();
        assert!((avg - 20.0).abs() < 0.01);
    }

    #[test]
    fn test_aggregate_min_max() {
        let mut pipe = AggregatePipeline::new();
        pipe.add_aggregator(Box::new(BuiltinAggregate::min("speed")));
        pipe.add_aggregator(Box::new(BuiltinAggregate::max("speed")));

        for s in [30.0, 80.0, 10.0, 90.0] {
            let mut props = HashMap::new();
            props.insert("speed".into(), PropertyValue::Float(s));
            pipe.process(props).unwrap();
        }

        let results = pipe.finalize();
        assert_eq!(results.get("min").and_then(|v| v.as_f64()), Some(10.0));
        assert_eq!(results.get("max").and_then(|v| v.as_f64()), Some(90.0));
    }

    #[test]
    fn test_reset() {
        let mut pipe = AggregatePipeline::new();
        pipe.add_aggregator(Box::new(BuiltinAggregate::count()));

        for _ in 0..3 {
            pipe.process(HashMap::new()).unwrap();
        }
        assert_eq!(
            pipe.finalize().get("count"),
            Some(&PropertyValue::Integer(3))
        );

        pipe.reset();
        assert_eq!(
            pipe.finalize().get("count"),
            Some(&PropertyValue::Integer(0))
        );
    }
}
