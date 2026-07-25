//! Distance metrics for vector comparison.

use serde::{Deserialize, Serialize};

/// Supported distance/similarity metrics.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Distance {
    /// Euclidean (L2) distance
    L2,
    /// Cosine similarity (1 - cosine)
    Cosine,
    /// Inner product (dot product, higher = more similar)
    InnerProduct,
}

/// Compute L2 (Euclidean) distance: sqrt(Σ(a_i - b_i)²)
pub fn euclidean_distance(a: &[f32], b: &[f32]) -> f32 {
    a.iter()
        .zip(b.iter())
        .map(|(x, y)| (x - y).powi(2))
        .sum::<f32>()
        .sqrt()
}

/// Compute cosine similarity: a·b / (|a| * |b|)
/// Returns 1 - cos_sim, so 0 = identical, 2 = opposite
pub fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
    let norm_a: f32 = a.iter().map(|x| x.powi(2)).sum::<f32>().sqrt();
    let norm_b: f32 = b.iter().map(|x| x.powi(2)).sum::<f32>().sqrt();
    let denom = norm_a * norm_b;
    if denom < 1e-12 {
        return 1.0;
    }
    1.0 - dot / denom // 0 = identical, 2 = opposite
}

/// Compute inner product: Σ a_i * b_i (negated so higher = closer)
pub fn inner_product(a: &[f32], b: &[f32]) -> f32 {
    let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
    -dot // Negate: higher inner product = lower "distance"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_euclidean_identical_vectors() {
        let a = [1.0, 2.0, 3.0];
        let b = [1.0, 2.0, 3.0];
        assert!((euclidean_distance(&a, &b) - 0.0).abs() < 1e-6);
    }

    #[test]
    fn test_euclidean_orthogonal_vectors() {
        let a = [1.0, 0.0];
        let b = [0.0, 1.0];
        assert!((euclidean_distance(&a, &b) - std::f32::consts::SQRT_2).abs() < 1e-5);
    }

    #[test]
    fn test_euclidean_known_distance() {
        let a = [0.0, 0.0, 0.0];
        let b = [3.0, 4.0, 0.0];
        assert!((euclidean_distance(&a, &b) - 5.0).abs() < 1e-6);
    }

    #[test]
    fn test_euclidean_single_element() {
        let a = [5.0];
        let b = [2.0];
        assert!((euclidean_distance(&a, &b) - 3.0).abs() < 1e-6);
    }

    #[test]
    fn test_cosine_identical_vectors() {
        let a = [1.0, 2.0, 3.0];
        let b = [1.0, 2.0, 3.0];
        // 1 - cos(0) = 1 - 1 = 0
        assert!((cosine_similarity(&a, &b) - 0.0).abs() < 1e-6);
    }

    #[test]
    fn test_cosine_opposite_vectors() {
        let a = [1.0, 0.0];
        let b = [-1.0, 0.0];
        // 1 - cos(π) = 1 - (-1) = 2
        assert!((cosine_similarity(&a, &b) - 2.0).abs() < 1e-6);
    }

    #[test]
    fn test_cosine_orthogonal_vectors() {
        let a = [1.0, 0.0];
        let b = [0.0, 1.0];
        // 1 - cos(π/2) = 1 - 0 = 1
        assert!((cosine_similarity(&a, &b) - 1.0).abs() < 1e-6);
    }

    #[test]
    fn test_cosine_zero_vector() {
        let a = [0.0, 0.0];
        let b = [1.0, 1.0];
        // Zero vector → denom = 0 → return 1.0
        assert!((cosine_similarity(&a, &b) - 1.0).abs() < 1e-6);
    }

    #[test]
    fn test_cosine_both_zero_vectors() {
        let a = [0.0, 0.0];
        let b = [0.0, 0.0];
        assert!((cosine_similarity(&a, &b) - 1.0).abs() < 1e-6);
    }

    #[test]
    fn test_inner_product_identical() {
        let a = [1.0, 2.0, 3.0];
        let b = [1.0, 2.0, 3.0];
        // dot = 14, negated = -14
        assert!((inner_product(&a, &b) - (-14.0)).abs() < 1e-6);
    }

    #[test]
    fn test_inner_product_orthogonal() {
        let a = [1.0, 0.0];
        let b = [0.0, 1.0];
        // dot = 0, negated = 0
        assert!((inner_product(&a, &b) - 0.0).abs() < 1e-6);
    }

    #[test]
    fn test_inner_product_negative_values() {
        let a = [1.0, -2.0];
        let b = [3.0, 4.0];
        // dot = 3 - 8 = -5, negated = 5
        assert!((inner_product(&a, &b) - 5.0).abs() < 1e-6);
    }

    #[test]
    fn test_distance_enum_serde() {
        let d1 = Distance::L2;
        let json = serde_json::to_string(&d1).unwrap();
        let d2: Distance = serde_json::from_str(&json).unwrap();
        assert_eq!(d1, d2);

        let d3 = Distance::Cosine;
        let json = serde_json::to_string(&d3).unwrap();
        let d4: Distance = serde_json::from_str(&json).unwrap();
        assert_eq!(d3, d4);

        let d5 = Distance::InnerProduct;
        let json = serde_json::to_string(&d5).unwrap();
        let d6: Distance = serde_json::from_str(&json).unwrap();
        assert_eq!(d5, d6);
    }

    #[test]
    fn test_distance_enum_debug() {
        assert_eq!(format!("{:?}", Distance::L2), "L2");
        assert_eq!(format!("{:?}", Distance::Cosine), "Cosine");
        assert_eq!(format!("{:?}", Distance::InnerProduct), "InnerProduct");
    }

    #[test]
    fn test_euclidean_empty_vectors() {
        let a: [f32; 0] = [];
        let b: [f32; 0] = [];
        assert!((euclidean_distance(&a, &b) - 0.0).abs() < 1e-6);
    }

    #[test]
    fn test_cosine_single_element() {
        let a = [5.0];
        let b = [3.0];
        // Both positive, same direction → cosine = 1 → distance = 0
        assert!((cosine_similarity(&a, &b) - 0.0).abs() < 1e-6);
    }
}
