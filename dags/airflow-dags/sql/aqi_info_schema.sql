CREATE TABLE IF NOT EXISTS aqi_info (
  device_id text NOT NULL PRIMARY KEY,
  time timestamptz NOT NULL,
  longitude text NOT NULL,
  latitude text NOT NULL,
  pm2_5 float NOT NULL
);
CREATE INDEX IF NOT EXISTS sqi_info_time_index ON aqi_info USING btree (time);
