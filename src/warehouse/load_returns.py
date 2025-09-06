from pathlib import Path
import pandas as pd
import psycopg

RAW_DIR = Path("data/raw")
HORIZONS = [1, 5, 21]  # días adelante

def compute_returns(ticker: str) -> pd.DataFrame:
    df = pd.read_parquet(RAW_DIR / f"{ticker}.parquet")
    df = df.rename(columns={"Adj Close": "adj_close", "Date": "date"})
    df["date"] = pd.to_datetime(df["date"]).dt.date

    out = []
    for h in HORIZONS:
        # desplazamos adj_close -h posiciones
        df[f"adj_close_h{h}"] = df["adj_close"].shift(-h)
        df[f"ret_{h}"] = df[f"adj_close_h{h}"] / df["adj_close"] - 1
        tmp = df[["date", f"ret_{h}"]].dropna()
        tmp["horizon"] = h
        tmp["ticker"] = ticker
        tmp = tmp.rename(columns={f"ret_{h}": "ret_pct"})
        out.append(tmp[["ticker", "date", "horizon", "ret_pct"]])
    return pd.concat(out)

def upsert_returns(conn, df: pd.DataFrame):
    rows = [
        (r.ticker, r.date, int(r.horizon), float(r.ret_pct))
        for r in df.itertuples(index=False)
    ]
    sql = """
        INSERT INTO fct_return (ticker,date,horizon,ret_pct)
        VALUES (%s,%s,%s,%s)
        ON CONFLICT (ticker,date,horizon) DO UPDATE
          SET ret_pct = EXCLUDED.ret_pct
    """
    with conn.cursor() as cur:
        cur.executemany(sql, rows)

if __name__ == "__main__":
    conn = psycopg.connect(
        "dbname=predacc user=postgres password=postgres host=localhost port=5433"
    )
    tickers = [p.stem for p in RAW_DIR.glob("*.parquet")]

    with conn:
        for t in tickers:
            df = compute_returns(t)
            upsert_returns(conn, df)
            print(f"✓ {t}: {len(df)} retornos cargados")
