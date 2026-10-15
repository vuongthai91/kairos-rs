//! Static service discovery — returns the query string interpreted as
//! a literal `host:port` pair (or just a host, in which case
//! `default_port` is used).
//!
//! The static source is mostly useful as a fallback or for testing.
//! In production you almost always want DNS or Consul. We still
//! implement it for completeness so the same `DiscoveryManager` can
//! drive all three source types.

use super::DiscoverySpec;
use crate::models::router::Backend;

/// Build a single backend from the spec's query string.
///
/// The format is `host` (using `default_port`) or `host:port`. The
/// host is prefixed with the spec's scheme if it isn't already
/// present.
pub async fn resolve(spec: &DiscoverySpec) -> Result<Vec<Backend>, String> {
    if spec.query.is_empty() {
        // Caller is expected to fall back to the route's
        // hard-coded backends in this case.
        return Ok(vec![]);
    }
    let (host, port) = split_host_port(&spec.query, spec.default_port);
    let host = if host.contains("://") {
        host
    } else {
        format!("{}://{}", spec.scheme, host)
    };
    Ok(vec![Backend {
        host,
        port,
        weight: 1,
        health_check_path: None,
        tls: None,
    }])
}

fn split_host_port(s: &str, default_port: u16) -> (String, u16) {
    // Strip a scheme prefix if present, so we can find the colon
    // separating host from port.
    let s = s
        .strip_prefix("http://")
        .or_else(|| s.strip_prefix("https://"))
        .or_else(|| s.strip_prefix("ws://"))
        .or_else(|| s.strip_prefix("wss://"))
        .unwrap_or(s);
    match s.rsplit_once(':') {
        Some((h, p)) => match p.parse::<u16>() {
            Ok(port) => (h.to_string(), port),
            Err(_) => (s.to_string(), default_port),
        },
        None => (s.to_string(), default_port),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(query: &str) -> DiscoverySpec {
        spec_with_scheme(query, "http")
    }

    fn spec_with_scheme(query: &str, scheme: &str) -> DiscoverySpec {
        DiscoverySpec {
            source: super::super::DiscoverySource::Static,
            query: query.to_string(),
            scheme: scheme.to_string(),
            default_port: 80,
            ttl_secs: 30,
            consul_url: None,
        }
    }

    #[tokio::test]
    async fn empty_query_returns_empty() {
        let out = resolve(&spec("")).await.unwrap();
        assert!(out.is_empty());
    }

    #[tokio::test]
    async fn host_only_uses_default_port() {
        let out = resolve(&spec("backend.local")).await.unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].host, "http://backend.local");
        assert_eq!(out[0].port, 80);
    }

    #[tokio::test]
    async fn host_with_explicit_port() {
        let out = resolve(&spec("backend.local:8080")).await.unwrap();
        assert_eq!(out[0].host, "http://backend.local");
        assert_eq!(out[0].port, 8080);
    }

    #[tokio::test]
    async fn host_with_scheme_is_preserved() {
        let out =
            resolve(&spec_with_scheme("https://api.example.com:443", "https")).await.unwrap();
        assert_eq!(out[0].host, "https://api.example.com");
        assert_eq!(out[0].port, 443);
    }

    #[tokio::test]
    async fn invalid_port_falls_back_to_default() {
        let out = resolve(&spec("backend.local:notaport")).await.unwrap();
        // The whole string stays as the host because the part after
        // `:` didn't parse as u16.
        assert_eq!(out[0].host, "http://backend.local:notaport");
        assert_eq!(out[0].port, 80);
    }
}