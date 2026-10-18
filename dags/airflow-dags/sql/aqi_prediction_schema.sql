-- One row per station, image and model: the inputs that went into the
-- prediction and what the model returned. Times are Asia/Taipei local time,
-- like aqi4_info.time.
CREATE TABLE IF NOT EXISTS aqi_prediction (
    site_id          TEXT        NOT NULL,
    sitename         TEXT        NOT NULL,
    image_time       TIMESTAMP   NOT NULL,   -- when the station camera took the photo
    image_key        TEXT        NOT NULL,   -- object key in the MinIO bucket aqi-images
    weather_time     TIMESTAMP   NOT NULL,   -- hour of the weather values below
    rh               DOUBLE PRECISION,
    rainfall         DOUBLE PRECISION,
    temperature      DOUBLE PRECISION,
    wd_hr            DOUBLE PRECISION,
    ws_hr            DOUBLE PRECISION,
    model_name       TEXT        NOT NULL,
    pred_class       INTEGER     NOT NULL,   -- 0 Good ... 5 Severe
    pred_label       TEXT        NOT NULL,
    regression_value DOUBLE PRECISION,
    predicted_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (site_id, image_time, model_name)
);

CREATE INDEX IF NOT EXISTS aqi_prediction_image_time_index
    ON aqi_prediction USING btree (image_time);

-- model_version is what the inference service returns ("taiwan_v1", ...).
-- Rows from before the column existed: model A served by vqaweb before
-- 2026-10-18 20:38 (Taipei) came from the untrained "legacy" model.
ALTER TABLE aqi_prediction ADD COLUMN IF NOT EXISTS model_version TEXT;
UPDATE aqi_prediction
   SET model_version = CASE WHEN predicted_at < '2026-10-18 20:38:00+08' THEN 'legacy' ELSE 'taiwan_v1' END
 WHERE model_version IS NULL;
