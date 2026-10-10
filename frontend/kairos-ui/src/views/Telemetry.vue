<script setup lang="ts">
import { ref, onMounted, computed } from 'vue';
import { apiService } from '../services/api';
import type { TelemetryStatus } from '../types/telemetry';

const status = ref<TelemetryStatus | null>(null);
const loading = ref(true);
const error = ref<string | null>(null);

async function loadStatus(): Promise<void> {
  loading.value = true;
  error.value = null;
  try {
    status.value = await apiService.getTelemetryStatus();
  } catch (e) {
    error.value = e instanceof Error ? e.message : String(e);
  } finally {
    loading.value = false;
  }
}

onMounted(loadStatus);

const statusBadgeClass = computed(() => {
  if (!status.value) return '';
  if (!status.value.enabled) return 'badge-disabled';
  if (status.value.initialized) return 'badge-enabled';
  return 'badge-error';
});

const statusLabel = computed(() => {
  if (!status.value) return '';
  if (!status.value.enabled) return 'Disabled';
  if (status.value.initialized) return 'Enabled · Exporter Live';
  return 'Enabled · Exporter Not Live';
});

const sampleRatioPercent = computed(() => {
  if (!status.value) return '—';
  return `${(status.value.sample_ratio * 100).toFixed(0)}%`;
});
</script>

<template>
  <div class="telemetry-view">
    <header class="view-header">
      <h1>OpenTelemetry Tracing</h1>
      <p class="view-subtitle">
        Read-only view of the gateway's distributed tracing state.
        Edit <code>config.json</code> and reload the backend to change
        these settings.
      </p>
    </header>

    <div v-if="loading" class="card loading-card">
      <p>Loading telemetry status…</p>
    </div>

    <div v-else-if="error" class="card error-card">
      <h2>Failed to load telemetry status</h2>
      <p class="error-message">{{ error }}</p>
      <button class="retry-btn" @click="loadStatus">Retry</button>
    </div>

    <template v-else-if="status">
      <section class="card status-card">
        <div class="status-row">
          <span class="field-label">Status</span>
          <span class="badge" :class="statusBadgeClass">
            {{ statusLabel }}
          </span>
        </div>

        <div class="status-row">
          <span class="field-label">OTLP Endpoint</span>
          <code class="field-value">
            {{ status.otlp_endpoint ?? '— not configured —' }}
          </code>
        </div>

        <div class="status-row">
          <span class="field-label">Service Name</span>
          <code class="field-value">{{ status.service_name }}</code>
        </div>

        <div class="status-row">
          <span class="field-label">Sample Ratio</span>
          <code class="field-value">
            {{ status.sample_ratio.toFixed(2) }} ({{ sampleRatioPercent }})
          </code>
        </div>

        <div class="status-row">
          <span class="field-label">Tracer Provider</span>
          <code class="field-value">
            {{ status.initialized ? 'Initialized' : 'Not Initialized' }}
          </code>
        </div>
      </section>

      <section class="card info-card">
        <h2>How it works</h2>
        <ul>
          <li>
            <strong>Root span</strong>: every incoming request gets a
            <code>http.request</code> span with method, path, scheme,
            host, user-agent, client IP, and final status code.
          </li>
          <li>
            <strong>Upstream span</strong>: the
            <code>RouteHandler</code> creates a child
            <code>http.upstream</code> span around the reqwest call,
            with the URL, method, and upstream status.
          </li>
          <li>
            <strong>W3C propagation</strong>: inbound
            <code>traceparent</code> is honored as the parent context
            when present; otherwise the gateway starts a fresh trace
            (subject to the sample ratio).
          </li>
          <li>
            <strong>Response headers</strong>: every response carries
            <code>X-Trace-Id</code> and <code>X-Span-Id</code> so
            clients can correlate with backend logs.
          </li>
        </ul>
        <p class="note">
          <strong>To enable:</strong> set
          <code>telemetry.enabled = true</code> and
          <code>telemetry.otlp_endpoint</code> to a real OTLP/gRPC
          collector (e.g. <code>http://otel-collector:4317</code>) in
          <code>config.json</code>, then restart the gateway or call
          <code>POST /api/config/reload</code>.
        </p>
      </section>
    </template>
  </div>
</template>

<style scoped>
.telemetry-view {
  max-width: 960px;
  margin: 0 auto;
  padding: 1.5rem;
}

.view-header {
  margin-bottom: 1.5rem;
}

.view-header h1 {
  font-size: 1.6rem;
  font-weight: 600;
  margin: 0 0 0.4rem 0;
  color: var(--color-text-primary, #e6e6e6);
}

.view-subtitle {
  color: var(--color-text-secondary, #a0a0a0);
  margin: 0;
  font-size: 0.9rem;
}

.view-subtitle code {
  background: rgba(255, 255, 255, 0.06);
  padding: 0.1rem 0.35rem;
  border-radius: 3px;
  font-size: 0.85em;
}

.card {
  background: var(--color-surface, #1e1e22);
  border: 1px solid var(--color-border, #2a2a30);
  border-radius: 8px;
  padding: 1.25rem 1.5rem;
  margin-bottom: 1.25rem;
}

.loading-card,
.error-card {
  text-align: center;
  color: var(--color-text-secondary, #a0a0a0);
}

.error-card {
  border-color: #b34a4a;
}

.error-message {
  font-family: monospace;
  background: rgba(255, 0, 0, 0.08);
  padding: 0.5rem;
  border-radius: 4px;
  color: #ff8a8a;
}

.retry-btn {
  margin-top: 0.75rem;
  background: var(--color-accent, #4a8cff);
  color: #fff;
  border: none;
  padding: 0.5rem 1.25rem;
  border-radius: 4px;
  cursor: pointer;
}

.retry-btn:hover {
  filter: brightness(1.1);
}

.status-card .status-row {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 0.7rem 0;
  border-bottom: 1px solid var(--color-border, #2a2a30);
}

.status-card .status-row:last-child {
  border-bottom: none;
}

.field-label {
  font-weight: 500;
  color: var(--color-text-secondary, #a0a0a0);
}

.field-value {
  font-family: monospace;
  color: var(--color-text-primary, #e6e6e6);
}

.badge {
  padding: 0.25rem 0.7rem;
  border-radius: 12px;
  font-size: 0.8rem;
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: 0.05em;
}

.badge-enabled {
  background: rgba(74, 222, 128, 0.15);
  color: #4ade80;
  border: 1px solid rgba(74, 222, 128, 0.3);
}

.badge-disabled {
  background: rgba(160, 160, 160, 0.1);
  color: #a0a0a0;
  border: 1px solid rgba(160, 160, 160, 0.25);
}

.badge-error {
  background: rgba(255, 138, 138, 0.12);
  color: #ff8a8a;
  border: 1px solid rgba(255, 138, 138, 0.3);
}

.info-card h2 {
  font-size: 1.05rem;
  font-weight: 600;
  margin: 0 0 0.75rem 0;
  color: var(--color-text-primary, #e6e6e6);
}

.info-card ul {
  margin: 0 0 1rem 0;
  padding-left: 1.25rem;
  color: var(--color-text-secondary, #b8b8b8);
  line-height: 1.55;
}

.info-card li {
  margin-bottom: 0.5rem;
}

.info-card code {
  background: rgba(255, 255, 255, 0.06);
  padding: 0.05rem 0.3rem;
  border-radius: 3px;
  font-size: 0.88em;
}

.note {
  background: rgba(74, 140, 255, 0.08);
  border-left: 3px solid var(--color-accent, #4a8cff);
  padding: 0.6rem 0.9rem;
  border-radius: 4px;
  font-size: 0.88rem;
  margin: 0;
  color: var(--color-text-primary, #e6e6e6);
}

.note strong {
  color: var(--color-accent, #4a8cff);
}
</style>
