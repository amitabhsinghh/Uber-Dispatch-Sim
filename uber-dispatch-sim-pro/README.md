# Uber-Style Dispatch Simulator (Starter)

**Services**
- `simulator` (Python): generates drivers & ride requests; publishes to Kafka (`RIDES_TOPIC`).
- `dispatch` (Go): consumes rides, picks a driver from Redis pool, persists assignment to Postgres, publishes to `ASSIGNMENTS_TOPIC`.
- `gateway` (Node/Express): REST to fetch assignments and basic health checks.

**Infra**
- Kafka, Zookeeper, Postgres, Redis via Docker Compose.

## Quick start
```bash
# 1) In VS Code: open this folder
# 2) Build+run
docker compose up --build

# In another terminal: check assignments
curl http://localhost:8080/assignments
curl http://localhost:8080/health
```

## Offer-level Upgrades Included

- **Match quality:** Redis GEO + Haversine ETA. Seed with:
  ```bash
  python3 utils/seed_drivers.py 500
  ```
- **Backpressure/Resilience:** retry-once then **dead-letter** to `${ASSIGNMENTS_DEAD_TOPIC}` when no drivers; chaos toggles:
  - `CHAOS_DROP_KAFKA=true` – drop assignment publishes
  - `CHAOS_FAIL_ASSIGN_PERCENT=30` – randomly fail 30% of assignments
- **Observability:** Prometheus at `:9090`, Grafana at `:3000` (anonymous login). Dispatch metrics on `:9100/metrics`, Gateway metrics on `:9101/metrics`.
- **Scale tests:** `k6 run perf/k6-assignments.js` or `locust -f perf/locustfile.py --host=http://localhost:8080`.
- **Algorithm variants:** `MATCH_STRATEGY=greedy` (default) and placeholder `hungarian` path (greedy batch).

## Run with Observability
```bash
docker compose up --build
python3 utils/seed_drivers.py 500
# open http://localhost:3000 to view the dashboard
```

## Metrics to watch
- `dispatch_assignments_total`
- `dispatch_assign_latency` (use p95/p99 in Grafana)
- `dispatch_retries_total`, `dispatch_deadletter_total`
- `gateway_http_requests_total`

### New: Micro-batched Hungarian Matching + Surge + Tracing
- **Micro-batches** (size=20 or 250ms): build a **cost matrix** of distances and solve via **Hungarian** (simplified; good for small N).
- **Geo-aware surge pricing**: per-ride multiplier from local ride/driver density within 3km. Stored as `surge_mult`, `price` in DB.
- **Simulated travel-time**: ETA varies with time-of-day (rush hours slower).
- **OpenTelemetry**: set `OTEL_EXPORTER_OTLP_ENDPOINT=tempo:4317` to ship traces. Grafana already has a Tempo datasource.
- **k6 scenarios**: see `perf/k6-sustained.js` and `perf/k6-spike.js`.

#### Run with tracing
```bash
export OTEL_EXPORTER_OTLP_ENDPOINT=tempo:4317
docker compose up --build
python3 utils/seed_drivers.py 500
# Grafana: http://localhost:3000 (Prom + Tempo)
```
