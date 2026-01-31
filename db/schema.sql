
-- Core tables
CREATE TABLE IF NOT EXISTS indicators (
  indicator_code TEXT PRIMARY KEY,
  indicator_name TEXT NOT NULL,
  unit TEXT,
  geo_level TEXT,
  source TEXT,
  topic TEXT,
  methodology TEXT,
  license TEXT,
  update_frequency TEXT,
  first_period TEXT,
  last_period TEXT,
  is_active BOOLEAN DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS series_meta (
  series_id BIGSERIAL PRIMARY KEY,
  indicator_code TEXT REFERENCES indicators(indicator_code),
  ref_area TEXT,             -- ISO3 or admin code
  sex TEXT,
  age TEXT,
  other_disagg JSONB,
  source TEXT,
  last_check TIMESTAMP WITH TIME ZONE DEFAULT now()
);

CREATE TABLE IF NOT EXISTS datapoints (
  series_id BIGINT REFERENCES series_meta(series_id),
  period TEXT,               -- "YYYY" or "YYYYQx" or date
  value NUMERIC,
  obs_status TEXT,
  last_updated TIMESTAMP WITH TIME ZONE,
  PRIMARY KEY (series_id, period)
);

-- For provenance and reproducibility
CREATE TABLE IF NOT EXISTS ingestion_log (
  id BIGSERIAL PRIMARY KEY,
  indicator_code TEXT,
  job_id TEXT,
  started_at TIMESTAMP WITH TIME ZONE,
  finished_at TIMESTAMP WITH TIME ZONE,
  status TEXT,
  records_ingested INTEGER,
  details JSONB
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_datapoints_series_period ON datapoints(series_id, period);
CREATE INDEX IF NOT EXISTS idx_series_meta_indicator ON series_meta(indicator_code);
