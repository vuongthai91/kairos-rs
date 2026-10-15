//! Consul-based service discovery.
//!
//! Calls the Consul agent's HTTP catalog API to fetch healthy service
//! instances:
//!
//! ```text
//! GET <consul_url>/v1/health/service/<service>?passing=1
//! ```
//!
//! The response is a JSON array; each element has a `Service` object
//! with `Address` and `Port`. We return one [`Backend`] per instance.
//!
//! The default Consul URL is `http://127.0.0.1:8500`, but most
//! production deployments override it via the spec's `consul_url`
//! field.

use super::DiscoverySpec;
use crate::models::router::Backend;
use serde::Deserialize;

/// Health endpoint response shape. We only deserialize the fields we
/// need; the rest is ignored.
#[derive(Debug, Deserialize)]
struct HealthResponse {
    #[serde(default, rename = "Service")]
    service: Option<ServiceEntry>,
}

#[derive(Debug, Deserialize)]
struct ServiceEntry {
    /// IP or hostname of the instance.
    #[serde(default, alias = "Address", alias = "address")]
    address: String,
    /// Port the instance is listening on.
    #[serde(default, alias = "Port", alias = "port")]
    port: u16,
    /// Optional Consul-assigned weight.
    #[serde(default, alias = "Weight", alias = "weight")]
    weight: Option<u32>,
}

/// Resolve the spec against a Consul agent.
///
/// `http` is the shared [`reqwest::Client`] from the manager (so DNS
/// + TCP setup happens once and we get connection pooling across
/// refreshes).
pub async fn resolve(http: &reqwest::Client, spec: &DiscoverySpec) -> Result<Vec<Backend>, String> {
    let base = spec
        .consul_url
        .clone()
        .unwrap_or_else(|| "http://127.0.0.1:8500".to_string());
    let url = format!(
        "{}/v1/health/service/{}?passing=1",
        base.trim_end_matches('/'),
        urlencode(&spec.query),
    );
    let resp = http
        .get(&url)
        .send()
        .await
        .map_err(|e| format!("consul: GET {} failed: {}", url, e))?;
    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(format!(
            "consul: GET {} returned {}: {}",
            url, status, body
        ));
    }
    let entries: Vec<HealthResponse> = resp
        .json()
        .await
        .map_err(|e| format!("consul: failed to parse response: {}", e))?;
    let backends: Vec<Backend> = entries
        .into_iter()
        .filter_map(|e| e.service)
        .filter(|s| !s.address.is_empty())
        .map(|s| Backend {
            host: format!("{}://{}", spec.scheme, s.address),
            port: s.port,
            weight: s.weight.unwrap_or(1).max(1),
            health_check_path: None,
            tls: None,
        })
        .collect();
    if backends.is_empty() {
        return Err(format!("consul: no healthy instances for service '{}'", spec.query));
    }
    Ok(backends)
}

/// Percent-encode a Consul service name. Consul accepts alphanumerics
/// + a few separators, but to be safe we encode anything that isn't
/// `[A-Za-z0-9._-]`.
fn urlencode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        let c = b as char;
        if c.is_ascii_alphanumeric() || c == '.' || c == '_' || c == '-' {
            out.push(c);
        } else {
            out.push_str(&format!("%{:02X}", b));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn urlencode_handles_typical_service_names() {
        assert_eq!(urlencode("api"), "api");
        assert_eq!(urlencode("my-service"), "my-service");
        assert_eq!(urlencode("api.v1"), "api.v1");
        assert_eq!(urlencode("api gateway"), "api%20gateway");
        assert_eq!(urlencode("svc&test"), "svc%26test");
    }

    #[tokio::test]
    async fn unreachable_consul_returns_error() {
        // Pick a port we know is closed so the connection refused
        // error fires immediately (no DNS resolution).
        let spec = DiscoverySpec {
            source: super::super::DiscoverySource::Consul,
            query: "api".to_string(),
            scheme: "http".to_string(),
            default_port: 80,
            ttl_secs: 30,
            consul_url: Some("http://127.0.0.1:1".to_string()),
        };
        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_millis(500))
            .build()
            .unwrap();
        let out = resolve(&http, &spec).await;
        assert!(out.is_err());
    }

    #[tokio::test]
    async fn consul_404_is_reported_as_error() {
        // Spin up a tiny mock server that always 404s.
        use tokio::net::TcpListener;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 1024];
            let _ = sock.read(&mut buf).await;
            sock.write_all(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n")
                .await
                .unwrap();
        });
        let spec = DiscoverySpec {
            source: super::super::DiscoverySource::Consul,
            query: "api".to_string(),
            scheme: "http".to_string(),
            default_port: 80,
            ttl_secs: 30,
            consul_url: Some(format!("http://{}", addr)),
        };
        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_millis(500))
            .build()
            .unwrap();
        let out = resolve(&http, &spec).await;
        assert!(out.is_err());
        let _ = handle.await;
    }

    #[tokio::test]
    async fn consul_200_with_empty_array_fails_closed() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let body = "[]";
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
            body.len(),
            body
        );
        let handle = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 1024];
            let _ = sock.read(&mut buf).await;
            sock.write_all(response.as_bytes()).await.unwrap();
        });
        let spec = DiscoverySpec {
            source: super::super::DiscoverySource::Consul,
            query: "api".to_string(),
            scheme: "http".to_string(),
            default_port: 80,
            ttl_secs: 30,
            consul_url: Some(format!("http://{}", addr)),
        };
        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_millis(500))
            .build()
            .unwrap();
        let out = resolve(&http, &spec).await;
        // No healthy instances -> fail-closed, not Ok with empty list.
        assert!(out.is_err());
        let _ = handle.await;
    }

    #[tokio::test]
    async fn consul_200_with_one_instance_returns_one_backend() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let body = r#"[{"Service":{"Address":"10.0.0.5","Port":8080,"Weight":3}}]"#;
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
            body.len(),
            body
        );
        let handle = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 1024];
            let _ = sock.read(&mut buf).await;
            sock.write_all(response.as_bytes()).await.unwrap();
        });
        let spec = DiscoverySpec {
            source: super::super::DiscoverySource::Consul,
            query: "api".to_string(),
            scheme: "https".to_string(),
            default_port: 80,
            ttl_secs: 30,
            consul_url: Some(format!("http://{}", addr)),
        };
        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_millis(500))
            .build()
            .unwrap();
        let out = resolve(&http, &spec).await;
        let v = out.unwrap();
        assert_eq!(v.len(), 1);
        assert_eq!(v[0].host, "https://10.0.0.5");
        assert_eq!(v[0].port, 8080);
        assert_eq!(v[0].weight, 3);
        let _ = handle.await;
    }
}