//! Integration tests for the response-compression middleware (PR5).
use kairos_rs::middleware::compression::{
    build, CompressionAlgorithm, CompressionConfig,
};

#[test]
fn test_default_config_is_valid_and_buildable() {
    let cfg = CompressionConfig::default();
    assert!(cfg.enabled);
    assert_eq!(cfg.level, 6);
    assert_eq!(cfg.min_size, 1024);
    assert!(cfg.validate().is_ok());
    assert!(build(&cfg).is_some());
}

#[test]
fn test_build_returns_none_when_disabled() {
    let mut cfg = CompressionConfig::default();
    cfg.enabled = false;
    assert!(build(&cfg).is_none());
}

#[test]
fn test_is_compressible_type() {
    let mut cfg = CompressionConfig::default();
    cfg.content_types = vec!["text/*".to_string(), "application/json".to_string()];
    assert!(cfg.is_compressible_type("text/plain"));
    assert!(cfg.is_compressible_type("application/json"));
    assert!(!cfg.is_compressible_type("image/png"));
    assert!(!cfg.is_compressible_type(""));
}

#[test]
fn test_validate_rejects_bad_level() {
    let mut cfg = CompressionConfig::default();
    cfg.level = 0;
    assert!(cfg.validate().is_err());
    cfg.level = 10;
    assert!(cfg.validate().is_err());
    cfg.level = 6;
    assert!(cfg.validate().is_ok());
}

#[test]
fn test_validate_rejects_empty_content_types() {
    let mut cfg = CompressionConfig::default();
    cfg.content_types.clear();
    assert!(cfg.validate().is_err());
}

#[test]
fn test_validate_rejects_empty_algorithms() {
    let mut cfg = CompressionConfig::default();
    cfg.algorithms.clear();
    assert!(cfg.validate().is_err());
    cfg.algorithms = vec![CompressionAlgorithm::Gzip];
    assert!(cfg.validate().is_ok());
}

#[test]
fn test_algorithm_tokens() {
    assert_eq!(CompressionAlgorithm::Gzip.token(), "gzip");
    assert_eq!(CompressionAlgorithm::Brotli.token(), "br");
    assert_eq!(CompressionAlgorithm::Deflate.token(), "deflate");
}

#[test]
fn test_compression_uses_default_encoding_when_algorithms_empty() {
    let mut cfg = CompressionConfig::default();
    cfg.algorithms = vec![];
    assert!(cfg.validate().is_err(), "empty algorithms must fail validation");
}

#[test]
fn test_compression_uses_first_algorithm_as_default() {
    let cfg = CompressionConfig {
        enabled: true,
        level: 6,
        min_size: 1024,
        content_types: vec!["text/*".to_string()],
        algorithms: vec![CompressionAlgorithm::Brotli],
    };
    assert!(cfg.validate().is_ok());
    assert!(build(&cfg).is_some());
    assert_eq!(cfg.algorithms[0], CompressionAlgorithm::Brotli);
}
