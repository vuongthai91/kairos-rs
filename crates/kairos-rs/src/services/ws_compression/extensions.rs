//! `Sec-WebSocket-Extensions` header parser (RFC 6455 § 4.3 + RFC 7692).
//!
//! The header value is a comma-separated list of extension offers,
//! each with optional semicolon-separated parameters. For example:
//!
//! ```text
//! permessage-deflate; server_no_context_takeover; client_max_window_bits=12
//! ```
//!
//! This module parses that into structured [`WebSocketExtension`]
//! values, so the negotiation logic in [`super::CompressionConfig`]
//! can compare offers without re-parsing the string each time.

/// `permessage-deflate` extension name (RFC 7692).
pub const PERMESSAGE_DEFLATE: &str = "permessage-deflate";

/// A single WebSocket extension offer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WebSocketExtension {
    /// Extension name, lowercased.
    pub name: String,
    /// Parameters in original order.
    pub params: Vec<ExtensionParam>,
}

/// A single `name` or `name=value` parameter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExtensionParam {
    /// Parameter name, lowercased.
    pub name: String,
    /// Parameter value if present, lowercased.
    pub value: Option<String>,
}

impl WebSocketExtension {
    /// `true` if a parameter with the given name is present (with
    /// any value, including no value).
    pub fn has_param(&self, name: &str) -> bool {
        self.params.iter().any(|p| p.name == name)
    }

    /// `Some(value)` if a parameter is present and has a value.
    pub fn param(&self, name: &str) -> Option<&str> {
        self.params
            .iter()
            .find(|p| p.name == name)
            .and_then(|p| p.value.as_deref())
    }
}

/// Parse a `Sec-WebSocket-Extensions` header value into the
/// individual extension offers. Empty / whitespace-only inputs
/// return an empty list. Tokens are LOWER-CASED on the way in
/// (RFC 6455 § 4.3 says they're case-insensitive).
///
/// Whitespace handling: HTTP header values are allowed to have
/// optional whitespace around commas and semicolons; we strip it
/// before splitting.
pub fn parse_extensions_header(value: &str) -> Vec<WebSocketExtension> {
    let mut out = Vec::new();
    for ext_str in value.split(',') {
        let ext_str = ext_str.trim();
        if ext_str.is_empty() {
            continue;
        }
        let mut tokens = ext_str.split(';');
        let name = match tokens.next() {
            Some(n) => n.trim().to_ascii_lowercase(),
            None => continue,
        };
        if name.is_empty() {
            continue;
        }
        let mut params = Vec::new();
        for tok in tokens {
            let tok = tok.trim();
            if tok.is_empty() {
                continue;
            }
            let (pname, pvalue) = match tok.split_once('=') {
                Some((n, v)) => (n.trim().to_ascii_lowercase(), Some(v.trim().trim_matches('"').to_ascii_lowercase())),
                None => (tok.to_ascii_lowercase(), None),
            };
            if pname.is_empty() {
                continue;
            }
            params.push(ExtensionParam { name: pname, value: pvalue });
        }
        out.push(WebSocketExtension { name, params });
    }
    out
}

/// What we agreed on with the peer. We only model permessage-deflate
/// parameters here; adding a new extension means a new field.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NegotiatedExtensions {
    /// The backend (or client, depending on direction) reset its
    /// zlib state after every server→client message.
    pub server_no_context_takeover: bool,
    /// The client reset its zlib state after every client→server
    /// message.
    pub client_no_context_takeover: bool,
    /// Negotiated window bits (8..=15). `None` means the default
    /// 15 was accepted.
    pub window_bits: Option<u8>,
}

impl NegotiatedExtensions {
    /// Build the `Sec-WebSocket-Extensions` header value that
    /// announces the negotiation to the other side.
    pub fn to_header_value(&self) -> String {
        let mut parts = vec![PERMESSAGE_DEFLATE.to_string()];
        if self.server_no_context_takeover {
            parts.push("server_no_context_takeover".to_string());
        }
        if self.client_no_context_takeover {
            parts.push("client_no_context_takeover".to_string());
        }
        if let Some(w) = self.window_bits {
            if (8..=15).contains(&w) {
                parts.push(format!("server_max_window_bits={}", w));
                parts.push(format!("client_max_window_bits={}", w));
            }
        }
        parts.join("; ")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_empty_input() {
        assert!(parse_extensions_header("").is_empty());
        assert!(parse_extensions_header("   ").is_empty());
    }

    #[test]
    fn parses_single_extension_without_params() {
        let exts = parse_extensions_header("permessage-deflate");
        assert_eq!(exts.len(), 1);
        assert_eq!(exts[0].name, "permessage-deflate");
        assert!(exts[0].params.is_empty());
    }

    #[test]
    fn parses_extension_with_params() {
        let exts = parse_extensions_header(
            "permessage-deflate; server_no_context_takeover; client_max_window_bits=12",
        );
        assert_eq!(exts.len(), 1);
        let ext = &exts[0];
        assert_eq!(ext.name, "permessage-deflate");
        assert!(ext.has_param("server_no_context_takeover"));
        assert_eq!(ext.param("server_no_context_takeover"), None);
        assert_eq!(ext.param("client_max_window_bits"), Some("12"));
    }

    #[test]
    fn parses_multiple_extensions() {
        let exts =
            parse_extensions_header("permessage-deflate; server_no_context_takeover, x-foo; bar=1");
        assert_eq!(exts.len(), 2);
        assert_eq!(exts[0].name, "permessage-deflate");
        assert_eq!(exts[1].name, "x-foo");
        assert_eq!(exts[1].param("bar"), Some("1"));
    }

    #[test]
    fn is_case_insensitive() {
        let exts = parse_extensions_header("PERMESSAGE-DEFLATE; Server_No_Context_Takeover");
        assert_eq!(exts.len(), 1);
        assert_eq!(exts[0].name, "permessage-deflate");
        assert!(exts[0].has_param("server_no_context_takeover"));
    }

    #[test]
    fn strips_quoted_param_values() {
        let exts = parse_extensions_header(r#"foo; bar="baz""#);
        assert_eq!(exts[0].param("bar"), Some("baz"));
    }

    #[test]
    fn negotiated_extensions_to_header_value() {
        let neg = NegotiatedExtensions {
            server_no_context_takeover: true,
            client_no_context_takeover: false,
            window_bits: Some(10),
        };
        let s = neg.to_header_value();
        assert!(s.starts_with("permessage-deflate"));
        assert!(s.contains("server_no_context_takeover"));
        assert!(!s.contains("client_no_context_takeover"));
        assert!(s.contains("server_max_window_bits=10"));
        assert!(s.contains("client_max_window_bits=10"));
    }
}
