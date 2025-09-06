#src/flow/etl_flow.py
from datetime import datetime
from pathlib import Path
import os
import pandas as pd
import yfinance as yf
import psycopg
from prefect import flow, task, get_run_logger

# --- COnfig (puede ir en un .env )

RAW_DIR = Path(os.getenv("RAW_DIR", "data/raw"))
RAW_DIR.mkdir(parents=True, exist_ok=True)

PG_DSN = os.getenv(
    "PG_DSN",
    "dbname=predacc user=postgres password=postgres host=localhost port=5433"
)

# --- HElpers -----

def _validate(df: pd.DataFrame) -> list[str]:
    errs = []
    expected = ["Date", "Open", "High", "Low", "Close", "Adj Close", "Volume"]
    missing = [c for c in expected if c not in df.columns]
    if missing:
        errs.append(f"Faltan columnas: {missing}")
        return errs

    if df["Date"].duplicated().any():
        errs.append("Fechas duplicadas")
    if not df["Date"].is_monotonic_increasing:
        errs.append("Fechas no están en orden ascendente")

    price_cols = ["Open", "High", "Low", "Close", "Adj Close"]
    if (df[price_cols] < 0).any().any():
        errs.append("Hay precios negativos")
    if (df["Volume"] < 0).any():
        errs.append("Hay volumen negativo")
    if df[price_cols + ["Volume"]].isna().any().any():
        errs.append("Hay NaN en precios/volumen")
    return errs


def _upsert_dim_asset(conn, ticker: str):
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO dim_asset (ticker) VALUES (%s) ON CONFLICT (ticker) DO NOTHING",
            (ticker,)
        )
        
def _upsert_fct_price(conn, ticker: str, df: pd.DataFrame):
    df = df.rename(columns={
        "Date":"date", "Open":"open", "High":"high", "Low":"low",
        "Close":"close", "Adj Close":"adj_close", "Volume":"volume"
    }).copy()
    df["date"] = pd.to_datetime(df["date"]).dt.date
    rows = [
        (ticker, r.date, float(r.open), float(r.high), float(r.low),
         float(r.close), float(r.adj_close), int(r.volume))
        for r in df[["date","open","high","low","close","adj_close","volume"]].itertuples(index=False)
    ]
    if not rows:
        return
    sql = """
        INSERT INTO fct_price (ticker,date,open,high,low,close,adj_close,volume)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (ticker,date) DO UPDATE SET
            open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low,
            close=EXCLUDED.close, adj_close=EXCLUDED.adj_close, volume=EXCLUDED.volume
    """
    with conn.cursor() as cur:
        cur.executemany(sql, rows)
        
    
def _compute_returns_rows(df: pd.DataFrame, ticker: str, horizons=(1,5,21)):
    df = df.rename(columns={"Adj Close":"adj_close", "Date":"date"}).copy()
    df["date"] = pd.to_datetime(df["date"]).dt.date
    out = []
    for h in horizons:
        fwd = df["adj_close"].shift(-h)
        ret = fwd / df["adj_close"] - 1
        tmp = pd.DataFrame({"ticker": ticker, "date": df["date"], "horizon": h, "ret_pct": ret})
        tmp = tmp.dropna()
        out.append(tmp)
    return pd.concat(out) if out else pd.DataFrame(columns=["ticker","date","horizon","ret_pct"])


def _upsert_fct_return(conn, returns_df: pd.DataFrame):
    if returns_df.empty:
        return
    rows = [(r.ticker, r.date, int(r.horizon), float(r.ret_pct))
            for r in returns_df.itertuples(index=False)]
    sql = """
        INSERT INTO fct_return (ticker,date,horizon,ret_pct)
        VALUES (%s,%s,%s,%s)
        ON CONFLICT (ticker,date,horizon) DO UPDATE SET ret_pct=EXCLUDED.ret_pct
    """
    with conn.cursor() as cur:
        cur.executemany(sql, rows)
        
# --- tasks ---
@task(retries=2, retry_delay_seconds=60, timeout_seconds=60*5)
def extract_one(ticker: str, period: str="5y", interval: str="1d") -> Path:
    logger = get_run_logger()
    df = yf.download(
        ticker, period=period, interval=interval,
        auto_adjust=False, progress=False, group_by="column"
    )
    if df.empty:
        raise ValueError(f"Sin datos para {ticker}")
    df = df.reset_index()
    # Aplana si viniera MultiIndex
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [c[0] if c[0] != "Date" else "Date" for c in df.columns]
    path = RAW_DIR / f"{ticker}.parquet"
    df.to_parquet(path, index=False)
    logger.info(f"[{ticker}] extract OK → {path} ({len(df)} filas)")
    return path

@task(retries=1, retry_delay_seconds=30)
def curate_one(parquet_path: Path) -> Path:
    logger = get_run_logger()
    df = pd.read_parquet(parquet_path)
    errs = _validate(df)
    if errs:
        raise ValueError(f"{parquet_path.name} falló validación: {errs}")
    logger.info(f"{parquet_path.name} validación OK")
    return parquet_path

@task(retries=2, retry_delay_seconds=30)
def load_one(ticker: str, parquet_path: Path) -> int:
    logger = get_run_logger()
    df = pd.read_parquet(parquet_path)
    with psycopg.connect(PG_DSN) as conn:
        _upsert_dim_asset(conn, ticker)
        _upsert_fct_price(conn, ticker, df)
    logger.info(f"[{ticker}] load OK → fct_price")
    return len(df)

@task(retries=2, retry_delay_seconds=30)
def compute_returns_one(ticker: str, parquet_path: Path) -> int:
    logger = get_run_logger()
    df = pd.read_parquet(parquet_path)
    rets = _compute_returns_rows(df, ticker, horizons=(1,5,21))
    with psycopg.connect(PG_DSN) as conn:
        _upsert_fct_return(conn, rets)
    logger.info(f"[{ticker}] compute_returns OK → fct_return ({len(rets)} filas)")
    return len(rets)

# ----------------- FLOW -----------------
@flow(name="pred-acciones-etl")
def etl_flow(
    tickers: list[str] = ("NVDA","MSFT","TSLA","AAPL","AMZN","META","VOO"),
    period: str = "5y",
    interval: str = "1d",
):
    logger = get_run_logger()
    logger.info(f"ETL start | tickers={tickers} period={period} interval={interval}")

    for t in tickers:
        # EJECUCIÓN SÍNCRONA (sin .submit)
        path = extract_one(t, period, interval)             # Path parquet
        ok_path = curate_one(path)                           # Valida o lanza error
        _ = load_one(t, ok_path)                             # UPSERT a fct_price
        _ = compute_returns_one(t, ok_path)                  # UPSERT a fct_return
        logger.info(f"[{t}] pipeline ✅")

    logger.info("ETL done 🚀")