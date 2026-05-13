# London Transport Unified Grafana — Architecture Plan

End-to-end pipeline that ingests every London bus stop's arrival predictions from the TfL Unified API, interpolates "ghost" vehicle positions between stops, persists them in PostGIS, and visualizes the live fleet in Grafana. Everything runs as a Docker Compose stack.

## Stack at a glance

| Concern | Component |
| --- | --- |
| Stream processor | Apache Flink 1.18.1 (DataStream, Java 11, shaded JAR) |
| Storage | PostgreSQL 16 + PostGIS 3.4 |
| Visualization | Grafana 11 with provisioned datasource + dashboard |
| Tick source | `python:3.10-slim` TCP service emitting `tick\n` every `TFL_POLL_INTERVAL_MS` |
| Build | Multi-stage Dockerfile (Maven → Flink runtime image) |
| Orchestration | `docker-compose.yml` with single shared `ltuf-flink-java:1.18.1` image |

## Service topology (`docker-compose.yml`)

```text
                         +------------------+
                         |   tick-feed      |  python TCP :9999, "tick\n" every 30s
                         +---------+--------+
                                   |
                                   v
+-----------+    +-----------+    +------------------+    +-------------+
| postgres  |<-->| jobmanager|<-->| taskmanager (x1) |--->| postgres    |
| (postgis) |    | (Flink)   |    |  Flink workers   |    |  bus_positions
+-----------+    +-----+-----+    +------------------+    +-------------+
       ^               |
       |               v
       |         +-----+------+
       |         | submit-job | runs `flink run -c TflBusStreamingJob`
       |         +------------+
       |
       |       +-----------+
       +-----> |  grafana  | provisioned PostGIS datasource + TfL dashboard
               +-----------+
```

The `submit-job` container waits (via `curl`) for the JobManager REST `/overview` to report `taskmanagers > 0` and `slots-total > 0` before submitting `flink-tfl-bus.jar`.

## TfL discovery — full bus stop catalog

### Primary path: `/StopPoint/Mode/bus`

When `TFL_HUB_STOP_TYPES` is unset (the default), `HubSync.syncByMode` pages through TfL's canonical endpoint for every London bus stop:

```text
GET https://api.tfl.gov.uk/StopPoint/Mode/bus?page=N&app_key=...
```

- ~1000 rows per page, `total` field reported on each response.
- Loop stops cleanly when `seen >= total` or an empty page returns.
- Per-page log line: `StopPoint/Mode bus page=N rows=R accepted=A total=T`.
- Result: ~19k+ stops upserted into `hub_stops` in 500-row JDBC batches.

### Fallback paths (only if Mode discovery returns 0)

1. `/StopPoint/Type/{naptanType}/page/{n}` — paged StopPoint/Type discovery (probes both page=0 and page=1; Meta types selected via `JobParams.metaTypeMatchesAnyBusStop`).
2. `/StopPoint/Search/{q}?modes=bus` — narrow search for keywords (`"bus station"`, `"coach station"`, etc.) used as a last resort.

### JSON tolerance

`TflApiClient.extractStopPointRows` walks both top-level arrays and nested `stopPoints` / `stopPoint` / `matches` / `places` arrays. `HubRepository.parseIfBusHub` accepts numeric or string-encoded `lat`/`lon` so paged responses are not silently dropped.

## Streaming job — `TflBusStreamingJob`

Defined in `flink-tfl-bus/src/main/java/com/londontransport/flink/TflBusStreamingJob.java`.

```text
ticks: socketTextStream(tick-feed:9999) ── parallelism 1
   │
   ▼ flatMap(PollArrivalsFlatMap)         ── parallel TfL /Arrivals fan-out
arrivals: DataStream<Arrival>
   │ keyBy(vehicleId|lineName)
   ▼ process(GhostEnrichFunction)         ── stateful per-vehicle interpolation
positions: DataStream<BusPosition>
   │
   ▼ addSink(PostgisSink)                 ── JDBC INSERT with ST_MakePoint
```

- **Checkpointing**: every 60s; restart strategy = fixed-delay (8 attempts, 20s).
- **Ghost interpolation** (`GhostEnrichFunction`):
  - Loads `naptan_id → (lat, lon)` from `hub_stops`.
  - Per-vehicle `ValueState` tracks `(prev, next, leg_t0)`.
  - Emits a position lerp'd between `prev` and `next` based on `1 - tts / max(leg_t0, TFL_GHOST_DEFAULT_SEGMENT_SECS)`.
- **Arrivals polling** (`PollArrivalsFlatMap`):
  - Runs `TFL_ARRIVAL_POLL_PARALLELISM` concurrent `/Arrivals` calls per tick (default 32, clamped 1–64).
  - For ~19k stops at parallelism 48 the tick stays under the 30s budget; raise to 64 if needed.

## Database schema (`init/02-schema.sql`)

```sql
hub_stops (
  naptan_id PK, common_name, stop_type,
  lat DOUBLE PRECISION, lon DOUBLE PRECISION,
  hub_naptan_code, updated_at
)

bus_positions (
  time TIMESTAMPTZ DEFAULT now(),
  vehicle_id, route,
  location geometry(Point, 4326),
  bearing,
  next_naptan_id, prev_naptan_id,
  raw_time_to_station, ghost_fraction
)
```

Indexes: `idx_hub_stops_stop_type`, `idx_bus_positions_time` (DESC), `idx_bus_positions_vehicle_time`, GIST `idx_bus_positions_location`. PostGIS extension is enabled via `init/01-extensions.sql`.

## Grafana

Provisioned via `grafana/provisioning/`:

- **Datasource**: `TfL PostGIS` (uid `tfl-pg`) → `postgres:5432/tfl_bus`.
- **Dashboard** `tfl-bus-geomap` ("TfL buses (hub arrivals)"):
  - Table: row counts of `hub_stops` and `bus_positions`.
  - Geomap: latest position per `vehicle_id` via `DISTINCT ON (vehicle_id) … ORDER BY vehicle_id, time DESC` over `bus_positions`.
- Refresh 10s, default time window `now-15m`.

UI: `http://localhost:3000` (admin/admin).

## Configuration (env)

Compose reads `.env.local`. Job containers read these env vars at runtime:

| Variable | Default | Purpose |
| --- | --- | --- |
| `TFL_APP_KEY` | required | TfL Unified API primary key |
| `TFL_APP_ID` | optional | Only if registration uses app_id + app_key |
| `TFL_HUB_STOP_TYPES` | empty | Empty = full Mode/bus catalog. Set to comma list to force a Type-based subset. |
| `TFL_POLL_INTERVAL_MS` | `30000` | Tick cadence; also drives `tick-feed` |
| `TFL_ARRIVAL_POLL_PARALLELISM` | `32` | Concurrent `/Arrivals` HTTP calls per tick (1–64) |
| `TFL_GHOST_DEFAULT_SEGMENT_SECS` | `360` | Fallback segment length for ghost interpolation |
| `FLINK_JDBC_URL` | `jdbc:postgresql://postgres:5432/tfl_bus` | Postgres URL inside compose network |
| `JDBC_USER` / `JDBC_PASSWORD` | `tfl` / `tfl_secret` | DB creds |

Compose passes the same env block to `jobmanager`, `taskmanager`, and `submit-job` so the Java job (which only reads `System.getenv`) sees consistent values regardless of where the operator runs.

## Build & run

```powershell
cd e:\cursor_projects\london_transport_unified_grafana
docker compose --env-file .env.local build
docker compose --env-file .env.local up -d
docker compose --env-file .env.local logs -f submit-job jobmanager taskmanager
```

Verify once the job is running:

```powershell
docker compose --env-file .env.local exec postgres psql -U tfl -d tfl_bus -c "select count(*) from hub_stops;"
docker compose --env-file .env.local exec postgres psql -U tfl -d tfl_bus -c "select count(*) from bus_positions;"
```

UI endpoints:

- Flink: `http://localhost:8081`
- Grafana: `http://localhost:3000`
- Postgres (host): `localhost:5432` (`tfl/tfl_secret`, db `tfl_bus`)

## Operational notes

- **Throughput budget**: 19k stops ÷ 30s tick = ~633 RPS. With parallelism 48 each worker handles ~13 RPS — well under TfL rate limits and within HTTP/2 client capacity.
- **Idempotency**: `hub_stops` upsert uses `ON CONFLICT (naptan_id) DO UPDATE`, so re-running the sync is safe.
- **Cold start**: first run blocks the streaming graph until `HubSync.syncHubStops` completes; the JobManager reports `Bus stops synced: N` before `socketTextStream` starts.
- **Single TaskManager**: scale=1 today; raise the `taskmanager` scale and the parallelism of `tfl-hub-arrivals` if higher arrival fan-out is needed.

## File map

```text
docker-compose.yml                       Service graph (postgres, tick-feed, flink, grafana)
docker/Dockerfile.flink-job              Multi-stage Maven → Flink image
init/01-extensions.sql                   CREATE EXTENSION postgis
init/02-schema.sql                       hub_stops + bus_positions + indexes
grafana/provisioning/datasources/        PostGIS datasource
grafana/provisioning/dashboards/         dashboard.yml + tfl-buses.json
flink-tfl-bus/pom.xml                    Maven shade plugin (provided Flink + jackson + jdbc)
flink-tfl-bus/src/main/java/com/londontransport/flink/
  TflBusStreamingJob.java                main(): hub sync → tick → arrivals → ghost → sink
  JobParams.java                         Env config (hub types, poll interval, parallelism, JDBC)
  TflApiClient.java                      Unified API client (Mode, Type, Search, Arrivals, StopPoint)
  HubSync.java                           Mode-first discovery, Type/Search fallbacks
  HubRepository.java                     hub_stops upsert + naptan/lat-lon loaders
  PollArrivalsFlatMap.java               Tick → bounded thread-pool /Arrivals fan-out
  GhostEnrichFunction.java               Stateful per-vehicle position interpolation
  PostgisSink.java                       JDBC INSERT with ST_MakePoint into bus_positions
  Arrival.java / BusPosition.java        Plain serializable POJOs
  ArrivalParser.java                     TfL /Arrivals JSON → Arrival rows
```
