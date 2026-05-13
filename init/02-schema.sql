CREATE TABLE IF NOT EXISTS hub_stops (
    naptan_id       TEXT PRIMARY KEY,
    common_name     TEXT,
    stop_type       TEXT,
    lat             DOUBLE PRECISION NOT NULL,
    lon             DOUBLE PRECISION NOT NULL,
    hub_naptan_code TEXT,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_hub_stops_stop_type ON hub_stops (stop_type);

CREATE TABLE IF NOT EXISTS bus_positions (
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

CREATE INDEX IF NOT EXISTS idx_bus_positions_time ON bus_positions (time DESC);
CREATE INDEX IF NOT EXISTS idx_bus_positions_vehicle_time ON bus_positions (vehicle_id, time DESC);
CREATE INDEX IF NOT EXISTS idx_bus_positions_location ON bus_positions USING GIST (location);
