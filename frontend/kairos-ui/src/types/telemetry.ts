// PR8 — OpenTelemetry distributed tracing
//
// Mirrors the JSON shape returned by `GET /api/telemetry` in
// `crates/kairos-rs/src/routes/telemetry.rs`. Keep the two in
// sync — the integration test in
// `crates/kairos-rs/src/routes/telemetry.rs` pins the field
// names by parsing the response with `serde_json::Value`, so a
// rename here without updating the backend will break the test.

export interface TelemetryStatus {
  /** Master switch from `config.json > telemetry.enabled`. */
  enabled: boolean;
  /** OTLP/gRPC endpoint, `null` when telemetry is disabled or the
   *  operator didn't set one. */
  otlp_endpoint: string | null;
  /** `service.name` resource attribute stamped on every span. */
  service_name: string;
  /** Head-based sampler ratio (`0.0` = drop everything,
   *  `1.0` = always on). */
  sample_ratio: number;
  /** `true` when the global tracer provider is live and spans
   *  are being exported. `false` when `init_telemetry` was never
   *  called, returned `None`, or the guard was dropped. */
  initialized: boolean;
}
