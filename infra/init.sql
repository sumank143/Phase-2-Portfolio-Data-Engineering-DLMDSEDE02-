-- init.sql — Create aggregation tables for the streaming pipeline
-- Mounted at /docker-entrypoint-initdb.d/ for auto-execution on first start

CREATE TABLE IF NOT EXISTS accident_kpi_geo (
    event_date       DATE NOT NULL,
    lat_grid         DOUBLE PRECISION NOT NULL,
    lon_grid         DOUBLE PRECISION NOT NULL,
    total_accidents  BIGINT DEFAULT 0,
    fatal            BIGINT DEFAULT 0,
    serious          BIGINT DEFAULT 0,
    slight           BIGINT DEFAULT 0,
    total_casualties BIGINT DEFAULT 0,
    total_vehicles   BIGINT DEFAULT 0,
    PRIMARY KEY (event_date, lat_grid, lon_grid)
);

CREATE TABLE IF NOT EXISTS accident_conditions (
    event_date       DATE NOT NULL,
    weather          INTEGER NOT NULL,
    light            INTEGER NOT NULL,
    road_surface     INTEGER NOT NULL,
    speed_limit      INTEGER NOT NULL,
    total_accidents  BIGINT DEFAULT 0,
    fatal            BIGINT DEFAULT 0,
    severity_sum     BIGINT DEFAULT 0,
    PRIMARY KEY (event_date, weather, light, road_surface, speed_limit)
);

CREATE TABLE IF NOT EXISTS accident_hotspots (
    event_date                 DATE NOT NULL,
    local_authority_district   INTEGER NOT NULL,
    road_type                  INTEGER NOT NULL,
    urban_or_rural             INTEGER NOT NULL,
    total_accidents            BIGINT DEFAULT 0,
    weighted_severity          BIGINT DEFAULT 0,
    PRIMARY KEY (event_date, local_authority_district, road_type, urban_or_rural)
);

CREATE TABLE IF NOT EXISTS vehicle_profile (
    year                 INTEGER NOT NULL,
    age_band_of_driver   VARCHAR(100) NOT NULL,
    sex_of_driver        VARCHAR(50) NOT NULL,
    vehicle_type         VARCHAR(100) NOT NULL,
    vehicle_count        BIGINT DEFAULT 0,
    age_of_vehicle_sum   BIGINT DEFAULT 0,
    PRIMARY KEY (year, age_band_of_driver, sex_of_driver, vehicle_type)
);

-- Indexes for dashboard query performance
CREATE INDEX IF NOT EXISTS idx_kpi_geo_date ON accident_kpi_geo (event_date DESC);
CREATE INDEX IF NOT EXISTS idx_conditions_total ON accident_conditions (total_accidents DESC);
CREATE INDEX IF NOT EXISTS idx_hotspots_severity ON accident_hotspots (weighted_severity DESC);
CREATE INDEX IF NOT EXISTS idx_vehicle_count ON vehicle_profile (vehicle_count DESC);
