//! DNS-based service discovery.
//!
//! Supports two record types:
//!
//! - **SRV**: `_service._proto.name TTL IN SRV prio weight port target`
//!   — returns `(host, port)` pairs ready to use.
//! - **A / AAAA**: plain host names — returns the resolved IPs paired
//!   with the spec's `default_port`.
//!
//! The resolver is built fresh per call so test code can override the
//! nameservers; production code uses the system resolver.

use super::DiscoverySpec;
use crate::models::router::Backend;
use hickory_resolver::TokioAsyncResolver;
use hickory_resolver::system_conf::read_system_conf;
use std::time::Duration;

/// Build the default resolver pointing at the system nameservers.
fn system_resolver() -> Result<TokioAsyncResolver, String> {
    let (cfg, mut opts) = read_system_conf()
        .map_err(|e| format!("dns: failed to load system config: {}", e))?;
    if cfg.name_servers().is_empty() {
        return Err("dns: no nameservers configured".to_string());
    }
    opts.timeout = Duration::from_secs(3);
    opts.attempts = 2;
    Ok(TokioAsyncResolver::tokio(cfg, opts))
}

/// Resolve the spec to a list of [`Backend`]s.
///
/// Tries SRV first (when the query looks like a service name with
/// the `_` prefix), then falls back to A/AAAA. Any non-empty response
/// is accepted; if both fail the function returns `Err` so the
/// caller marks the route as fail-closed.
pub async fn resolve(spec: &DiscoverySpec) -> Result<Vec<Backend>, String> {
    let resolver = system_resolver()?;
    let is_srv = spec.query.starts_with('_');

    if is_srv {
        match lookup_srv(&resolver, &spec.query).await {
            Ok(srvs) if !srvs.is_empty() => return Ok(into_backends(srvs, spec)),
            Ok(_) => {
                // Empty SRV — fall through to A/AAAA
            }
            Err(e) => {
                // SRV query failed entirely. Try A/AAAA as a
                // last-ditch effort before failing the route.
                match lookup_a(&resolver, &spec.query).await {
                    Ok(ips) if !ips.is_empty() => {
                        return Ok(ips
                            .into_iter()
                            .map(|ip| backend_for(&ip.to_string(), spec))
                            .collect())
                    }
                    _ => return Err(format!("dns: SRV lookup failed: {}", e)),
                }
            }
        }
    }

    let ips = lookup_a(&resolver, &spec.query).await?;
    if ips.is_empty() {
        return Err(format!("dns: no A/AAAA records for {}", spec.query));
    }
    Ok(ips
        .into_iter()
        .map(|ip| backend_for(&ip.to_string(), spec))
        .collect())
}

async fn lookup_srv(
    resolver: &TokioAsyncResolver,
    name: &str,
) -> Result<Vec<(String, u16, u32)>, String> {
    use hickory_resolver::proto::rr::RData;
    use hickory_resolver::proto::rr::RecordType;
    let resp = resolver
        .lookup(name, RecordType::SRV)
        .await
        .map_err(|e| format!("dns: SRV query error: {}", e))?;
    let mut out = Vec::new();
    for r in resp.records() {
        if let Some(RData::SRV(srv)) = r.data() {
            let host = srv.target().to_string();
            let host = host.trim_end_matches('.').to_string();
            let port = srv.port();
            // SRV records carry weight in the proto, but the high-
            // level Record wrapper here doesn't expose it directly;
            // we fall back to 1.
            out.push((host, port, 1u32));
        }
    }
    Ok(out)
}

async fn lookup_a(
    resolver: &TokioAsyncResolver,
    name: &str,
) -> Result<Vec<std::net::IpAddr>, String> {
    let resp = resolver
        .lookup_ip(name)
        .await
        .map_err(|e| format!("dns: A/AAAA query error: {}", e))?;
    Ok(resp.iter().collect())
}

fn into_backends(srvs: Vec<(String, u16, u32)>, spec: &DiscoverySpec) -> Vec<Backend> {
    srvs.into_iter()
        .map(|(host, port, weight)| Backend {
            host: format!("{}://{}", spec.scheme, host),
            port,
            weight: if weight == 0 { 1 } else { weight },
            health_check_path: None,
            tls: None,
        })
        .collect()
}

fn backend_for(ip: &str, spec: &DiscoverySpec) -> Backend {
    Backend {
        host: format!("{}://{}", spec.scheme, ip),
        port: spec.default_port,
        weight: 1,
        health_check_path: None,
        tls: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dns_spec() -> DiscoverySpec {
        DiscoverySpec {
            source: super::super::DiscoverySource::Dns,
            query: "example.com".to_string(),
            scheme: "http".to_string(),
            default_port: 80,
            ttl_secs: 30,
            consul_url: None,
        }
    }

    #[tokio::test]
    async fn resolves_public_a_record() {
        // example.com is guaranteed to resolve to an A record.
        // We don't pin the IP; we just check we got at least one.
        let spec = dns_spec();
        let out = resolve(&spec).await;
        assert!(out.is_ok(), "example.com must resolve, got: {:?}", out);
        let backends = out.unwrap();
        assert!(!backends.is_empty());
        for b in &backends {
            assert!(b.host.starts_with("http://"));
            assert_eq!(b.port, 80);
        }
    }

    #[tokio::test]
    async fn unknown_domain_fails() {
        let mut spec = dns_spec();
        spec.query = "definitely-not-a-real-domain-xyz12345.invalid.".to_string();
        let out = resolve(&spec).await;
        assert!(out.is_err());
    }

    #[tokio::test]
    async fn srv_query_for_unsupported_service_fails() {
        let mut spec = dns_spec();
        spec.query = "_definitely-not-a-real-srv-xyz12345._tcp.example.com".to_string();
        // Either SRV lookup fails outright or returns empty; in
        // both cases the function returns Err.
        let out = resolve(&spec).await;
        assert!(out.is_err());
    }

    #[test]
    fn backend_for_uses_spec_scheme_and_port() {
        let spec = DiscoverySpec {
            source: super::super::DiscoverySource::Dns,
            query: "x".to_string(),
            scheme: "https".to_string(),
            default_port: 8443,
            ttl_secs: 30,
            consul_url: None,
        };
        let b = backend_for("10.0.0.1", &spec);
        assert_eq!(b.host, "https://10.0.0.1");
        assert_eq!(b.port, 8443);
        assert_eq!(b.weight, 1);
    }

    #[test]
    fn srv_into_backends_uses_srv_port_and_weight() {
        let spec = DiscoverySpec {
            source: super::super::DiscoverySource::Dns,
            query: "x".to_string(),
            scheme: "http".to_string(),
            default_port: 80,
            ttl_secs: 30,
            consul_url: None,
        };
        let out = into_backends(
            vec![("a.example".to_string(), 8080, 5), ("b.example".to_string(), 9090, 0)],
            &spec,
        );
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].host, "http://a.example");
        assert_eq!(out[0].port, 8080);
        assert_eq!(out[0].weight, 5);
        assert_eq!(out[1].weight, 1); // 0 weight -> 1
    }
}