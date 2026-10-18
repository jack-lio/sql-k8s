CREATE TABLE IF NOT EXISTS public.aqi6_info (
  device_id TEXT NOT NULL,
  time      TIMESTAMPTZ NOT NULL,
  longitude TEXT,
  latitude  TEXT,
  aqi       TEXT,
  pm2_5     TEXT,
  pm10      TEXT,
  o3        TEXT,
  co        TEXT,
  so2       TEXT,
  no2       TEXT,
  PRIMARY KEY (device_id, time)
);

CREATE INDEX IF NOT EXISTS idx_aqi6_info_time   ON public.aqi6_info (time);
CREATE INDEX IF NOT EXISTS idx_aqi6_info_device ON public.aqi6_info (device_id);

