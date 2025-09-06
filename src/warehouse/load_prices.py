# src/warehouse/load_prices.py
from pathlib import Path
import math
import pandas as pd
import psycopg

RAW_DIR = Path("data/raw")

def list_tickers():
    return [p.stem for p in RAW_DIR.glob("*.parquet")]

def load_parquet(ticker: str) -> pd.DataFrame:
    df = pd.read_parquet(RAW_DIR / f"{ticker}.parquet")
    df = df.rename(columns={
        "Date": "date",
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Adj Close": "adj_close",
        "Volume": "volume",
    })
    df["date"] = pd.to_datetime(df["date"]).dt.date
    cols = ["date","open","high","low","close","adj_close","volume"]
    return df[cols]

def upsert_dim_asset(conn, tickers):
    rows = [(t,) for t in tickers]
    with conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO dim_asset (ticker) VALUES (%s) ON CONFLICT (ticker) DO NOTHING",
            rows
        )

def upsert_fct_price(conn, ticker, df: pd.DataFrame, chunk_size: int = 5000):
    # Preparamos filas tipadas
    rows = [
        (ticker, r.date, float(r.open), float(r.high), float(r.low),
         float(r.close), float(r.adj_close), int(r.volume))
        for r in df.itertuples(index=False)
    ]
    sql = """
        INSERT INTO fct_price (ticker,date,open,high,low,close,adj_close,volume)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (ticker, date) DO UPDATE
        SET open=EXCLUDED.open,
            high=EXCLUDED.high,
            low=EXCLUDED.low,
            close=EXCLUDED.close,
            adj_close=EXCLUDED.adj_close,
            volume=EXCLUDED.volume
    """
    with conn.cursor() as cur:
        if not rows:
            return
        # chunk para no saturar el statement
        n = len(rows)
        for i in range(0, n, chunk_size):
            cur.executemany(sql, rows[i:i+chunk_size])

if __name__ == "__main__":
    # OJO: usa el puerto que dejaste en docker-compose (te sugerí 5433)
    conn = psycopg.connect(
        "dbname=predacc user=postgres password=postgres host=localhost port=5433"
    )

    tickers = list_tickers()
    print("Tickers a cargar:", tickers)

    with conn:  # autocommit al salir si no hay error
        upsert_dim_asset(conn, tickers)
        for t in tickers:
            df = load_parquet(t)
            upsert_fct_price(conn, t, df)
            print(f"✓ {t}: {len(df)} filas cargadas")
