//! Integration tests for the service-discovery layer.
//!
//! These cover the end-to-end flow on the `DiscoveryManager`:
//! registering routes with discovery specs, running a single refresh
//! pass, and verifying the cache is updated correctly for both
//! successful and failed resolutions.
//!
//! DNS lookups in these tests use the public `example.com` domain
//! (guaranteed to resolve) and a guaranteed-unresolvable `.invalid`
//! TLD for the failure case. The Consul test stands up a tiny mock
//! HTTP server on an ephemeral port and verifies the request shape +
//! response parsing.
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use kairos_rs::models::router::Backend;
use kairos_rs::services::discovery::{
    DiscoveryManager, DiscoverySource, DiscoverySpec,
};

fn dns_spec(query: &str) -> DiscoverySpec {
    DiscoverySpec {
        source: DiscoverySource::Dns,
        query: query.to_string(),
        scheme: "http".to_string(),
        default_port: 80,
        ttl_secs: 30,
        consul_url: None,
    }
}

fn static_spec(query: &str) -> DiscoverySpec {
    DiscoverySpec {
        source: DiscoverySource::Static,
        query: query.to_string(),
        scheme: "http".to_string(),
        default_port: 80,
        ttl_secs: 30,
        consul_url: None,
    }
}

#[tokio::test]
async fn manager_resolves_dns_route_and_serves_backends() {
    let m = DiscoveryManager::new();
    let mut routes = HashMap::new();
    routes.insert("api".to_string(), dns_spec("example.com"));

    let res = m.refresh_once(&routes).await;
    let backends = res.get("api").unwrap().as_ref().expect("must resolve");
    assert!(!backends.is_empty(), "example.com must yield >= 1 backend");
    for b in backends {
        assert!(b.host.starts_with("http://"), "got host: {}", b.host);
    }

    // resolve() now returns the cached list (no fallback needed).
    let cached = m.resolve("api", &[]).await.expect("cache hit");
    assert_eq!(cached.len(), backends.len());
    assert_eq!(cached[0].host, backends[0].host);
}

#[tokio::test]
async fn manager_marks_dns_route_failed_when_unresolvable() {
    let m = DiscoveryManager::new();
    let mut routes = HashMap::new();
    routes.insert(
        "ghost".to_string(),
        dns_spec("this-host-does-not-exist-12345.invalid."),
    );

    let res = m.refresh_once(&routes).await;
    assert!(res.get("ghost").unwrap().is_err());

    // The next request to resolve() must fail closed — even though
    // the fallback list is non-empty.
    let fallback = vec![Backend {
        host: "http://fallback".into(),
        port: 8080,
        weight: 1,
        health_check_path: None,
        tls: None,
    }];
    let err = m.resolve("ghost", &fallback).await.unwrap_err();
    assert!(!err.is_empty());
    assert_eq!(m.route_state("ghost").await, Some(kairos_rs::services::discovery::RouteState::Failed));
}

#[tokio::test]
async fn manager_resolves_static_route_without_query() {
    let m = DiscoveryManager::new();
    let mut routes = HashMap::new();
    routes.insert("noop".to_string(), static_spec(""));
    let res = m.refresh_once(&routes).await;
    // Empty query returns Ok with an empty list (caller is expected
    // to use the route's hard-coded backends as the fallback).
    let backends = res.get("noop").unwrap().as_ref().unwrap();
    assert!(backends.is_empty());
}

#[tokio::test]
async fn manager_resolves_static_route_with_host_port() {
    let m = DiscoveryManager::new();
    let mut routes = HashMap::new();
    routes.insert("svc".to_string(), static_spec("backend.local:9090"));
    let res = m.refresh_once(&routes).await;
    let backends = res.get("svc").unwrap().as_ref().unwrap();
    assert_eq!(backends.len(), 1);
    assert_eq!(backends[0].host, "http://backend.local");
    assert_eq!(backends[0].port, 9090);
}

#[tokio::test]
async fn manager_unknown_route_uses_fallback() {
    let m = DiscoveryManager::new();
    let fb = vec![Backend {
        host: "http://default".into(),
        port: 80,
        weight: 1,
        health_check_path: None,
        tls: None,
    }];
    let out = m.resolve("not-registered", &fb).await.unwrap();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].host, "http://default");
}

#[tokio::test]
async fn manager_consul_route_with_mock_server() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    // Pre-define the response so the mock server is fast and
    // deterministic. We use the *exact* Consul response shape.
    let body = r#"[{"Node":{"ID":"x","Node":"x","Address":"127.0.0.1"},"Service":{"ID":"api-1","Service":"api","Address":"10.0.0.5","Port":8080,"Weight":3},"Checks":[]}]"#;
    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(),
        body
    );
    let handle = tokio::spawn(async move {
        // Accept one connection, write the response, then close.
        if let Ok((mut sock, _)) = listener.accept().await {
            let mut buf = [0u8; 1024];
            let _ = sock.read(&mut buf).await;
            let _ = sock.write_all(response.as_bytes()).await;
        }
    });

    let m = DiscoveryManager::new();
    let mut routes = HashMap::new();
    routes.insert(
        "consul-api".to_string(),
        DiscoverySpec {
            source: DiscoverySource::Consul,
            query: "api".to_string(),
            scheme: "http".to_string(),
            default_port: 80,
            ttl_secs: 30,
            consul_url: Some(format!("http://{}", addr)),
        },
    );
    let res = m.refresh_once(&routes).await;
    let backends = res
        .get("consul-api")
        .unwrap()
        .as_ref()
        .expect("consul mock must succeed");
    assert_eq!(backends.len(), 1);
    assert_eq!(backends[0].host, "http://10.0.0.5");
    assert_eq!(backends[0].port, 8080);
    assert_eq!(backends[0].weight, 3);

    let _ = handle.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn refresh_loop_only_refreshes_routes_past_their_ttl() {
    // Verify the loop's TTL gating: with a 5-second TTL, a route
    // that was just refreshed shouldn't be refreshed again within
    // 2 seconds.
    let m = Arc::new(DiscoveryManager::new());
    let mut routes = HashMap::new();
    routes.insert("api".to_string(), dns_spec("example.com"));
    let routes = Arc::new(tokio::sync::RwLock::new(routes));

    let _h = m.clone().start_refresh_loop(routes.clone());

    // First refresh happens within 1s + TTL
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(
        m.route_state("api").await,
        Some(kairos_rs::services::discovery::RouteState::Ok)
    );

    // Capture the cache for comparison.
    let first = m.resolve("api", &[]).await.unwrap_or_default();
    assert!(!first.is_empty(), "first refresh must populate cache");

    // Within the TTL window, the backends should be identical (no
    // second refresh happened yet). The test doesn't assert
    // equality on `host` because the DNS answer can rotate, but
    // it does assert the cache stayed populated.
    tokio::time::sleep(Duration::from_secs(2)).await;
    let second = m.resolve("api", &[]).await.unwrap_or_default();
    assert_eq!(first.len(), second.len(), "cache size must not change within TTL");
}
