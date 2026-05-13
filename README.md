# London Transport Unified Grafana

Real-time London bus tracking pipeline. Polls the entire TfL bus stop catalog (~19k+ stops) every 30 seconds, interpolates per-vehicle "ghost" positions between stops, persists them to PostGIS, and renders the live fleet on a Grafana geomap.

| Layer | Tech |
| --- | --- |
| Stream processor | Apache Flink 1.18.1 (Java 11 DataStream API, shaded JAR) |
| Storage | PostgreSQL 16 + PostGIS 3.4 |
| Visualization | Grafana 11 (provisioned datasource + dashboard) |
| Tick source | Lightweight Python TCP service emitting `tick\n` on the configured cadence |
| Orchestration | Docker Compose (single shared `ltuf-flink-java:1.18.1` image) |

---

## Architecture

```text
                              ┌───────────────┐
                              │   tick-feed   │  TCP :9999, "tick\n" every 30s
                              └───────┬───────┘
                                      │
                                      ▼
   ┌─────────────┐    ┌──────────────────────────┐    ┌──────────────────┐
   │  postgres   │◄──►│ jobmanager + taskmanager │◄──►│  postgres        │
   │  (postgis)  │    │  Flink streaming job     │───►│  bus_positions   │
   └─────┬───────┘    └────────────┬─────────────┘    └──────────────────┘
         │                         ▲
         │                         │
         │                  ┌──────┴───────┐
         │                  │  submit-job  │  waits for slots, runs flink jar
         │                  └──────────────┘
         │
         │            ┌───────────┐
         └───────────►│  grafana  │  PostGIS datasource + TfL dashboard
                      └───────────┘
```

`submit-job` polls `http://jobmanager:8081/overview` until a TaskManager with free slots is registered, then submits the shaded JAR via `flink run -c com.londontransport.flink.TflBusStreamingJob`.

---

## Core ETL logic

The streaming graph is defined in `flink-tfl-bus/src/main/java/com/londontransport/flink/TflBusStreamingJob.java`:

```text
ticks: socketTextStream(tick-feed:9999)        ── parallelism 1
   │
   ▼ flatMap(PollArrivalsFlatMap)              ── bounded thread-pool fan-out to TfL
arrivals: DataStream<Arrival>
   │ keyBy(vehicleId|lineName)
   ▼ process(GhostEnrichFunction)              ── stateful per-vehicle interpolation
positions: DataStream<BusPosition>
   │
   ▼ addSink(PostgisSink)                      ── JDBC INSERT with ST_MakePoint
```

### 1. Stop catalog discovery — `HubSync`

When `TFL_HUB_STOP_TYPES` is empty (the default), `HubSync.syncByMode` pages through TfL's canonical mode endpoint:

```text
GET https://api.tfl.gov.uk/StopPoint/Mode/bus?page=N&app_key=...
```

- Returns ~1000 rows per page; the response includes a `total` count.
- Loop terminates cleanly when `seen >= total` or an empty page is returned.
- Per-page progress is logged: `StopPoint/Mode bus page=N rows=R accepted=A total=T`.
- All bus-serving stops (`modes` contains `bus`, or `stopType` clearly indicates bus/coach/onstreet/interchange) are accepted.

If `TFL_HUB_STOP_TYPES` is set explicitly, or if Mode discovery returns nothing, `HubSync` falls back to:

1. **`/StopPoint/Type/{naptanType}/page/{n}`** — paged Type discovery against bus-related Meta types (probes both page=0 and page=1).
2. **`/StopPoint/Search/{q}?modes=bus`** — narrow keyword search (`"bus station"`, `"coach station"`, …) as a last resort.

The JSON parser walks both top-level arrays and nested `stopPoints` / `stopPoint` / `matches` / `places` arrays, and tolerates string-encoded `lat`/`lon`, so paged TfL payloads are never silently dropped.

Discovered stops are upserted into `hub_stops` in 500-row JDBC batches via `ON CONFLICT (naptan_id) DO UPDATE`, so re-running the sync is idempotent.

### 2. Tick-driven arrivals polling — `PollArrivalsFlatMap`

- The Python `tick-feed` service emits `tick\n` every `TFL_POLL_INTERVAL_MS` (default 30s) on TCP `:9999`.
- For each tick, `PollArrivalsFlatMap` issues `TFL_ARRIVAL_POLL_PARALLELISM` concurrent `/Arrivals` requests in waves until every NaPTAN id from `hub_stops` has been polled.
- Default parallelism is 32 (clamped 1–64). For ~19k stops, parallelism 48 keeps a full tick under the 30s budget.
- A bounded `ExecutorService` is created in `open()` and shut down in `close()`; per-stop failures are logged and skipped, never aborting the wave.
- Each TfL arrival row is parsed by `ArrivalParser` into an `Arrival` record carrying `(pollStop, vehicleId, lineName, timeToStationSecs, bearing)`.

### 3. Ghost interpolation — `GhostEnrichFunction`

Buses don't broadcast GPS via the Unified API; we synthesize position from arrival predictions.

- The function loads `naptan_id → (lat, lon)` from `hub_stops` once at `open()`.
- Stream is keyed by `vehicleId|lineName`. Per-key `ValueState<String>` stores the current "leg" as JSON: `{prev, next, leg_t0}`.
- On every arrival:
  1. `next` = the polling stop's NaPTAN.
  2. If `next` differs from the stored leg, we shift: `prev := old next`, `next := poll`, `leg_t0 := max(timeToStation, 60s)`.
  3. Otherwise we keep the leg and only widen `leg_t0` upward.
  4. Compute `tSeg = max(leg_t0, TFL_GHOST_DEFAULT_SEGMENT_SECS)` and `fraction = 1 - min(1, timeToStation / tSeg)`.
  5. Lerp lat/lon between the previous and next stops by `fraction`. Without a prev (first observation), snap to `next`.
- Emits a `BusPosition` with the interpolated coordinate, raw TTS, ghost fraction, and bearing.

This produces a smooth, monotonically advancing position that converges on the next stop as the predicted arrival approaches.

### 4. PostGIS sink — `PostgisSink`

- Single auto-commit JDBC connection per parallel sink instance.
- Writes via prepared statement:

```sql
INSERT INTO bus_positions
  (time, vehicle_id, route, location, bearing,
   next_naptan_id, prev_naptan_id, raw_time_to_station, ghost_fraction)
VALUES (?, ?, ?, ST_SetSRID(ST_MakePoint(?, ?), 4326), ?, ?, ?, ?, ?);
```

- Bearing is null when the arrival had no bearing field; `prev_naptan_id` is null on the first observation.

### Resilience knobs

- Checkpointing every 60s.
- Restart strategy: fixed-delay, 8 attempts, 20s delay.
- TfL HTTP client retries 5xx with exponential backoff; 429 fails fast (rate-limit guard).

---

## Database schema (`init/02-schema.sql`)

```sql
CREATE TABLE hub_stops (
  naptan_id       TEXT PRIMARY KEY,
  common_name     TEXT,
  stop_type       TEXT,
  lat             DOUBLE PRECISION NOT NULL,
  lon             DOUBLE PRECISION NOT NULL,
  hub_naptan_code TEXT,
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE bus_positions (
  time                 TIMESTAMPTZ NOT NULL DEFAULT now(),
  vehicle_id           TEXT NOT NULL,
  route                TEXT NOT NULL,
  location             geometry(Point, 4326) NOT NULL,
  bearing              REAL,
  next_naptan_id       TEXT,
  prev_naptan_id       TEXT,
  raw_time_to_station  INTEGER,
  ghost_fraction       REAL
);
```

Indexes: `idx_hub_stops_stop_type`, `idx_bus_positions_time` (DESC), `idx_bus_positions_vehicle_time`, GIST `idx_bus_positions_location`. The PostGIS extension is enabled by `init/01-extensions.sql`.

---

## Grafana

Provisioned out of `grafana/provisioning/`:

- **Datasource** `TfL PostGIS` (uid `tfl-pg`) → `postgres:5432/tfl_bus`.
- **Dashboard** `tfl-bus-geomap` ("TfL buses (hub arrivals)"):
  - Top table: live row counts of `hub_stops` and `bus_positions`.
  - Geomap: latest position per `vehicle_id` via `DISTINCT ON (vehicle_id) … ORDER BY vehicle_id, time DESC`.
- Refresh 10s, default time window `now-15m`.

UI: `http://localhost:3000` (admin / admin).

---

## Configuration

Compose reads `.env.local` (gitignored). Use `env.example` as a template:

```dotenv
TFL_APP_KEY=                   # required: TfL Unified API primary key
TFL_APP_ID=                    # optional: only if registration uses app_id + app_key

POSTGRES_USER=tfl
POSTGRES_PASSWORD=tfl_secret
POSTGRES_DB=tfl_bus
FLINK_JDBC_URL=jdbc:postgresql://postgres:5432/tfl_bus

TFL_HUB_STOP_TYPES=            # empty = full /StopPoint/Mode/bus catalog (~19k+)
TFL_GHOST_DEFAULT_SEGMENT_SECS=360
TFL_POLL_INTERVAL_MS=30000     # tick cadence; tick-feed uses the same value
TFL_ARRIVAL_POLL_PARALLELISM=32  # 1–64; raise to 48 for the full catalog

TFL_TICK_HOST=tick-feed
TFL_TICK_PORT=9999
```

The same env block is passed to `jobmanager`, `taskmanager`, and `submit-job` so the Java job (which only reads `System.getenv`) sees consistent values.

---

## Build & run

```powershell
cd e:\cursor_projects\london_transport_unified_grafana

# 1. Configure your TfL key
cp env.example .env.local
# edit .env.local and set TFL_APP_KEY=...

# 2. Build all Flink images (Maven shade happens inside the Dockerfile)
docker compose --env-file .env.local build

# 3. Start the stack
docker compose --env-file .env.local up -d

# 4. Watch the job submit and stream
docker compose --env-file .env.local logs -f submit-job jobmanager taskmanager
```

Expected log lines during cold start:

```text
StopPoint/Mode bus page=1 rows=1000 accepted=1000 total=19657
StopPoint/Mode bus page=2 rows=1000 accepted=2000 total=19657
...
Bus stops synced: 19657
Job has been submitted with JobID ...
```

Verify in Postgres once arrivals start flowing:

```powershell
docker compose --env-file .env.local exec postgres psql -U tfl -d tfl_bus -c `
  "select count(*) from hub_stops;"

docker compose --env-file .env.local exec postgres psql -U tfl -d tfl_bus -c `
  "select count(*) from bus_positions;"

docker compose --env-file .env.local exec postgres psql -U tfl -d tfl_bus -c `
  "select vehicle_id, route, time, ghost_fraction from bus_positions order by time desc limit 10;"
```

UI endpoints:

- Flink Web UI: `http://localhost:8081`
- Grafana: `http://localhost:3000`
- Postgres (host): `localhost:5432` (`tfl` / `tfl_secret`, db `tfl_bus`)

---

## Operational notes

- **Throughput budget**: 19k stops ÷ 30s tick = ~633 RPS. With `TFL_ARRIVAL_POLL_PARALLELISM=48`, each Java HTTP/2 client comfortably stays inside TfL's per-app rate limit.
- **Cold start ordering**: the streaming graph blocks until `HubSync.syncHubStops` finishes; `submit-job` blocks until at least one TaskManager slot is registered. This is intentional — no arrivals are polled until the catalog is in Postgres.
- **Idempotent reruns**: `hub_stops` upsert on `naptan_id`; `bus_positions` is append-only and indexed for `(time DESC)` and per-vehicle latest reads.
- **Single TaskManager today**: scale by raising the `taskmanager` service's `scale:` and the parallelism of `tfl-hub-arrivals` if you want larger parallel fan-out across machines.

---

## Repository layout

```text
docker-compose.yml                       Service graph (postgres, tick-feed, flink, grafana)
docker/Dockerfile.flink-job              Multi-stage Maven build → Flink runtime image
init/01-extensions.sql                   CREATE EXTENSION postgis
init/02-schema.sql                       hub_stops + bus_positions + indexes
grafana/provisioning/datasources/        PostGIS datasource (uid: tfl-pg)
grafana/provisioning/dashboards/         dashboard.yml + tfl-buses.json
flink-tfl-bus/pom.xml                    Maven shade build (provided Flink + jackson + jdbc)
flink-tfl-bus/src/main/java/com/londontransport/flink/
  TflBusStreamingJob.java                main(): hub sync → tick → arrivals → ghost → sink
  JobParams.java                         Env config (poll interval, parallelism, JDBC, …)
  TflApiClient.java                      Unified API client (Mode, Type, Search, Arrivals, StopPoint)
  HubSync.java                           Mode-first stop discovery, Type/Search fallbacks
  HubRepository.java                     hub_stops upsert + naptan/lat-lon loaders
  PollArrivalsFlatMap.java               Tick → bounded thread-pool /Arrivals fan-out
  GhostEnrichFunction.java               Stateful per-vehicle position interpolation
  PostgisSink.java                       JDBC INSERT with ST_MakePoint into bus_positions
  ArrivalParser.java                     TfL /Arrivals JSON → Arrival rows
  Arrival.java / BusPosition.java        Plain serializable POJOs
plan.md                                  Architecture / design notes
env.example                              Template for .env.local
```

---

## What changed (recent fixes that made this work end-to-end)

- **Switched stop discovery to `/StopPoint/Mode/bus`.** The previous Type-only path (`/StopPoint/Type/{naptanType}/page/{n}`) was returning empty payloads for the broad bus types, leaving the job in a 52-row search-fallback state. Mode discovery now reliably loads the full ~19k+ catalog.
- **Tolerant JSON parsing.** `extractStopPointRows` walks nested `stopPoints` / `stopPoint` / `matches` / `places` arrays, and `parseIfBusHub` accepts string-encoded `lat`/`lon`, so paged TfL responses are never silently dropped.
- **Concurrent arrivals fan-out.** `PollArrivalsFlatMap` runs a bounded `ExecutorService` (1–64 threads, configurable via `TFL_ARRIVAL_POLL_PARALLELISM`) so every stop is polled within a 30s tick at full catalog scale.
- **30s cadence wired end-to-end.** `TFL_POLL_INTERVAL_MS=30000` flows into `tick-feed`, `JobParams.pollIntervalMs`, and the per-tick wave scheduling.
- **Bash-based submit-job wait loop.** Replaced the previous PyFlink wait with a `curl` + `bash` poll on `/overview` (escaped `$$` for Compose) so the Flink runtime image no longer needs Python.
- **Batched, idempotent `hub_stops` upserts.** 500-row JDBC batches with `ON CONFLICT (naptan_id) DO UPDATE`, so cold restarts re-sync without duplicating or churning the table.
- **Same-image guarantee.** `jobmanager`, `taskmanager`, and `submit-job` share one anchored image (`ltuf-flink-java:1.18.1`) and an identical env block, eliminating the "stale TaskManager image" drift problem.
