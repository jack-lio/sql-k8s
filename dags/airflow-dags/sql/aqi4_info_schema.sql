CREATE TABLE IF NOT EXISTS aqi4_info (
    device_id TEXT,
    time     TIMESTAMP,
    longitude TEXT,         -- 原本是 DOUBLE PRECISION
    latitude  TEXT,         -- 原本是 DOUBLE PRECISION
    aqi       TEXT,         -- 原本是 INTEGER
    pm2_5     TEXT,         -- 原本是 DOUBLE PRECISION
    pm10      TEXT,         -- 原本是 DOUBLE PRECISION
    o3        TEXT,         -- 原本是 DOUBLE PRECISION
    co        TEXT,         -- 原本是 DOUBLE PRECISION
    so2       TEXT,         -- 原本是 DOUBLE PRECISION
    no2       TEXT,         -- 原本是 DOUBLE PRECISION
    PRIMARY KEY (device_id, time)
);

CREATE INDEX IF NOT EXISTS aqi4_info_time_index
    ON aqi4_info USING btree (time);
-- The WAQI stations in India were collected until 2026-10; only the four
-- Taiwan stations are now. Kariavattom kept returning a June 2026 reading,
-- which was re-inserted every hour after the 3-day cleanup deleted it.
DELETE FROM aqi4_info WHERE device_id NOT IN ('基隆', '林口', '嘉義', '安南');
