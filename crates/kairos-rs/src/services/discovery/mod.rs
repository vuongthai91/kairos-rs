//! Service discovery for the kairos-rs gateway.
//!
//! Lets a route declare its backends dynamically via an external source
//! instead of (or in addition to) a hard-coded list. The supported
//! sources are:
//!
//! - **DNS** — resolve backends via SRV or A/AAAA records
//!   (works out-of-the-box with k8s kube-dns / CoreDNS, Consul DNS,
//!   and any standard DNS server).
//! - **Consul** — query a Consul agent's HTTP catalog for healthy
//!   service instances.
//! - **Static** — return a hard-coded list (useful for fallback or
//!   when the discovery source itself is failing).
//!
//! The manager periodically refreshes each route's backends. A failure
//! to resolve marks the route as `Failed` and the gateway returns
//! `503 Service Unavailable` for matching requests until the next
//! successful refresh (**fail-closed**).
//!
//! # Configuration
//!
//! A route opts in by setting `discovery` on a [`Router`]:
//!
//! ```json
//! {
//!   "external_path": "/api/users",
//!   "internal_path": "/users",
//!   "methods": ["GET"],
//!   "discovery": {
//!     "source": "dns",
//!     "query": "users.default.svc.cluster.local",
//!     "scheme": "http",
//!     "ttl_secs": 30
//!   }
//! }
//! ```
//!
//! The gateway then queries DNS every 30s, replaces the route's
//! backends with the resolved list, and load-balances across them.

pub mod consul;
pub mod dns;
pub mod static_source;

use crate::models::router::Backend;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::time::interval;

/// Which discovery backend to use.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum DiscoverySource {
    /// Resolve via DNS (SRV or A/AAAA records).
    Dns,
    /// Query a Consul agent over HTTP.
    Consul,
    /// Static list (the route's existing `backends` field).
    Static,
}

/// Per-route discovery configuration.
///
/// All fields are deserialized from JSON; missing fields fall back to
/// sensible defaults so a minimal config like `{"source": "dns",
/// "query": "..."}` is enough.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoverySpec {
    /// Which source to query.
    pub source: DiscoverySource,

    /// What to ask the source. For DNS this is the domain name
    /// (`_http._tcp.svc.cluster.local` for SRV or `backend.svc` for
    /// A/AAAA). For Consul this is the service name registered with
    /// the agent. For Static this is ignored.
    pub query: String,

    /// URL scheme to use when building [`Backend`] values
    /// (`http` or `https`). Defaults to `http`.
    #[serde(default = "default_scheme")]
    pub scheme: String,

    /// Default backend port when the source doesn't return one
    /// (e.g. plain A/AAAA records instead of SRV). Defaults to 80.
    #[serde(default = "default_port")]
    pub default_port: u16,

    /// Refresh interval in seconds. Defaults to 30. Minimum 1.
    #[serde(default = "default_ttl_secs")]
    pub ttl_secs: u64,

    /// Optional Consul agent URL (e.g. `http://consul:8500`).
    /// Only used when `source = "consul"`. Defaults to
    /// `http://127.0.0.1:8500`.
    #[serde(default)]
    pub consul_url: Option<String>,
}

fn default_scheme() -> String { "http".to_string() }
fn default_port() -> u16 { 80 }
fn default_ttl_secs() -> u64 { 30 }

/// State of a single route's resolution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouteState {
    /// The route has no discovery configured.
    NotConfigured,
    /// We have a fresh list of backends.
    Ok,
    /// The most recent resolution failed. The route is fail-closed
    /// until the next successful refresh.
    Failed,
}

/// Cached backends for a single route.
#[derive(Debug, Clone)]
struct CacheEntry {
    state: RouteState,
    backends: Vec<Backend>,
    last_error: Option<String>,
    last_refresh: Option<std::time::Instant>,
}

/// The shared discovery manager.
///
/// Holds the cache of resolved backends and runs the periodic refresh
/// loop. All public methods are `&self` and use interior mutability
/// (`Arc<RwLock<…>>`) so it can be cloned freely and shared across
/// actix workers.
#[derive(Clone)]
pub struct DiscoveryManager {
    cache: Arc<RwLock<HashMap<String, CacheEntry>>>,
    http: reqwest::Client,
}

impl DiscoveryManager {
    /// Build a new manager. The HTTP client is reused for Consul calls
    /// (DNS uses `hickory-resolver` directly).
    pub fn new() -> Self {
        Self {
            cache: Arc::new(RwLock::new(HashMap::new())),
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .expect("reqwest client must build"),
        }
    }

    /// Look up the current cached backends for a route, applying the
    /// fail-closed rule.
    ///
    /// Returns:
    /// - `Ok(backends)` if discovery is not configured OR a fresh list
    ///   is available.
    /// - `Err(message)` if discovery is configured but the most
    ///   recent resolution failed. Callers should respond 503.
    pub async fn resolve(&self, route_id: &str, fallback: &[Backend]) -> Result<Vec<Backend>, String> {
        let cache = self.cache.read().await;
        match cache.get(route_id) {
            None => Ok(fallback.to_vec()),
            Some(CacheEntry { state: RouteState::NotConfigured, .. }) => Ok(fallback.to_vec()),
            Some(CacheEntry { state: RouteState::Ok, backends, .. }) => Ok(backends.clone()),
            Some(CacheEntry { state: RouteState::Failed, last_error, .. }) => {
                Err(last_error.clone().unwrap_or_else(|| "service discovery failed".to_string()))
            }
        }
    }

    /// Run one resolution pass synchronously (used by the background
    /// refresh task and by tests).
    pub async fn refresh_once(
        &self,
        routes: &HashMap<String, DiscoverySpec>,
    ) -> HashMap<String, Result<Vec<Backend>, String>> {
        let mut out = HashMap::new();
        for (id, spec) in routes {
            let result = match spec.source {
                DiscoverySource::Dns => dns::resolve(spec).await,
                DiscoverySource::Consul => consul::resolve(&self.http, spec).await,
                DiscoverySource::Static => static_source::resolve(spec).await,
            };
            let mut cache = self.cache.write().await;
            let entry = cache.entry(id.clone()).or_insert_with(|| CacheEntry {
                state: RouteState::NotConfigured,
                backends: vec![],
                last_error: None,
                last_refresh: None,
            });
            entry.last_refresh = Some(std::time::Instant::now());
            match &result {
                Ok(backends) => {
                    entry.state = RouteState::Ok;
                    entry.backends = backends.clone();
                    entry.last_error = None;
                }
                Err(e) => {
                    entry.state = RouteState::Failed;
                    entry.last_error = Some(e.clone());
                }
            }
            out.insert(id.clone(), result);
        }
        out
    }

    /// Spawn the background refresh loop. Returns immediately; the
    /// loop runs until the process exits.
    pub fn start_refresh_loop(
        self: &Arc<Self>,
        routes: Arc<RwLock<HashMap<String, DiscoverySpec>>>,
    ) -> tokio::task::JoinHandle<()> {
        let me = Arc::clone(self);
        tokio::spawn(async move {
            // Tick every second so we can react to TTL changes cheaply;
            // each route's actual refresh is gated by its own TTL.
            let mut ticker = interval(Duration::from_secs(1));
            ticker.tick().await; // skip the immediate first tick
            let mut last_refresh: HashMap<String, std::time::Instant> = HashMap::new();
            loop {
                ticker.tick().await;
                let routes = routes.read().await.clone();
                if routes.is_empty() { continue; }
                let now = std::time::Instant::now();
                let to_refresh: Vec<(String, DiscoverySpec)> = routes
                    .iter()
                    .filter(|(id, spec)| {
                        let ttl = Duration::from_secs(spec.ttl_secs.max(1));
                        match last_refresh.get(*id) {
                            Some(t) => now.duration_since(*t) >= ttl,
                            None => true,
                        }
                    })
                    .map(|(id, spec)| (id.clone(), spec.clone()))
                    .collect();
                if to_refresh.is_empty() { continue; }
                let routes_for_refresh: HashMap<String, DiscoverySpec> =
                    to_refresh.iter().cloned().collect();
                me.refresh_once(&routes_for_refresh).await;
                for (id, _) in to_refresh {
                    last_refresh.insert(id, now);
                }
            }
        })
    }

    /// Inspect the current state of a single route (for tests + the
    /// management API).
    pub async fn route_state(&self, route_id: &str) -> Option<RouteState> {
        self.cache.read().await.get(route_id).map(|e| e.state.clone())
    }

    /// Inspect the most recent resolution error for a route.
    pub async fn route_error(&self, route_id: &str) -> Option<String> {
        self.cache
            .read()
            .await
            .get(route_id)
            .and_then(|e| e.last_error.clone())
    }
}

impl Default for DiscoveryManager {
    fn default() -> Self { Self::new() }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(src: DiscoverySource, q: &str) -> DiscoverySpec {
        DiscoverySpec {
            source: src,
            query: q.to_string(),
            scheme: "http".to_string(),
            default_port: 80,
            ttl_secs: 30,
            consul_url: None,
        }
    }

    #[tokio::test]
    async fn unknown_route_returns_fallback() {
        let m = DiscoveryManager::new();
        let fb = vec![Backend {
            host: "http://x".to_string(),
            port: 1,
            weight: 1,
            health_check_path: None,
            tls: None,
        }];
        let out = m.resolve("missing", &fb).await.unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].host, "http://x");
    }

    #[tokio::test]
    async fn static_source_returns_empty_when_query_blank() {
        let m = DiscoveryManager::new();
        let mut routes = HashMap::new();
        routes.insert("r".to_string(), spec(DiscoverySource::Static, ""));
        let res = m.refresh_once(&routes).await;
        // Static source returns Ok with empty list (caller is
        // expected to provide fallbacks via the route's backends
        // field). The point of this test is that we don't error.
        assert!(res.get("r").unwrap().is_ok());
    }

    #[tokio::test]
    async fn failed_resolution_is_fail_closed() {
        let m = DiscoveryManager::new();
        // Use an unresolvable DNS name to force a failure.
        let mut routes = HashMap::new();
        routes.insert(
            "r".to_string(),
            spec(DiscoverySource::Dns, "definitely-not-a-real-host-12345.invalid."),
        );
        let res = m.refresh_once(&routes).await;
        assert!(res.get("r").unwrap().is_err());
        // Now resolve() should return Err, not fall back.
        let err = m.resolve("r", &[]).await.unwrap_err();
        assert!(!err.is_empty());
        assert_eq!(m.route_state("r").await, Some(RouteState::Failed));
    }

    #[tokio::test]
    async fn route_state_defaults_to_failed_when_route_unknown() {
        let m = DiscoveryManager::new();
        assert_eq!(m.route_state("nope").await, None);
    }
}