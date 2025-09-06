-- Dimensión de activos
CREATE TABLE IF NOT EXISTS dim_asset (
  asset_id   SERIAL PRIMARY KEY,
  ticker     TEXT UNIQUE NOT NULL
);

-- Precios diarios (hechos)
CREATE TABLE IF NOT EXISTS fct_price (
  ticker     TEXT NOT NULL,
  date       DATE NOT NULL,
  open       DOUBLE PRECISION,
  high       DOUBLE PRECISION,
  low        DOUBLE PRECISION,
  close      DOUBLE PRECISION,
  adj_close  DOUBLE PRECISION,
  volume     BIGINT,
  PRIMARY KEY (ticker, date),
  FOREIGN KEY (ticker) REFERENCES dim_asset(ticker) ON DELETE CASCADE
);

-- Retornos (hechos) - por horizonte (días)
CREATE TABLE IF NOT EXISTS fct_return (
  ticker     TEXT NOT NULL,
  date       DATE NOT NULL,          -- fecha base t
  horizon    INTEGER NOT NULL,       -- 1, 5, 21 ...
  ret_pct    DOUBLE PRECISION,       -- (adj_close[t+h] / adj_close[t] - 1)
  PRIMARY KEY (ticker, date, horizon),
  FOREIGN KEY (ticker) REFERENCES dim_asset(ticker) ON DELETE CASCADE
);

-- Pronósticos del modelo (hechos)
CREATE TABLE IF NOT EXISTS fct_forecast (
  ticker       TEXT NOT NULL,
  date         DATE NOT NULL,        -- fecha base t (cuando se emitió el forecast)
  horizon      INTEGER NOT NULL,     -- 1, 5, 21 ...
  y_pred       DOUBLE PRECISION,     -- predicción de adj_close[t+h]
  y_true       DOUBLE PRECISION,     -- valor real (si está disponible)
  model_name   TEXT,                 -- id del modelo/ruta mlflow
  run_id       TEXT,                 -- run de MLflow
  created_at   TIMESTAMP DEFAULT NOW(),
  PRIMARY KEY (ticker, date, horizon, COALESCE(run_id, 'default')),
  FOREIGN KEY (ticker) REFERENCES dim_asset(ticker) ON DELETE CASCADE
);

-- Índices útiles
CREATE INDEX IF NOT EXISTS idx_price_ticker_date ON fct_price (ticker, date DESC);
CREATE INDEX IF NOT EXISTS idx_return_ticker_date ON fct_return (ticker, date DESC);
CREATE INDEX IF NOT EXISTS idx_forecast_ticker_date ON fct_forecast (ticker, date DESC);

-- Vistas
CREATE OR REPLACE VIEW v_last_prices AS
SELECT DISTINCT ON (p.ticker)
  p.ticker, p.date AS last_date, p.adj_close AS last_price
FROM fct_price p
ORDER BY p.ticker, p.date DESC;

CREATE OR REPLACE VIEW v_cumulative_return AS
WITH base AS (
  SELECT
    ticker,
    date,
    adj_close,
    FIRST_VALUE(adj_close) OVER (PARTITION BY ticker ORDER BY date) AS first_price
  FROM fct_price
)
SELECT
  ticker,
  date,
  (adj_close / NULLIF(first_price, 0) - 1.0) AS cum_ret
FROM base;
