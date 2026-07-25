//! COPY protocol policy.
//!
//! COPY is not advertised until it can be implemented with bounded buffering,
//! cancellation, and transactional failure behavior. COPY statements currently
//! receive a feature-not-supported response through the SQL layer.
