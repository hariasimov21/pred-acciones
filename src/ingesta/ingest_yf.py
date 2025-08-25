import argparse
from pathlib import Path
import pandas as pd
import yfinance as yf

RAW_DIR = Path("data/raw")
RAW_DIR.mkdir(parents=True, exist_ok=True)

def validate(df: pd.DataFrame, ticker: str) -> list[str]:
    errs = []

    # Asegurar tipos y columnas esperadas
    expected = ["Date", "Open", "High", "Low", "Close", "Adj Close", "Volume"]
    missing = [c for c in expected if c not in df.columns]
    if missing:
        errs.append(f"Faltan columnas: {missing}")
        return errs  # sin estas columnas no seguimos

    # 1) Fechas duplicadas
    dup = df["Date"].duplicated().any()
    if bool(dup):
        errs.append("Fechas duplicadas")

    # 2) Fechas en orden ascendente
    mono = df["Date"].is_monotonic_increasing
    if not bool(mono):
        errs.append("Fechas no están en orden ascendente")

    # 3) No negativos en precios y volumen >= 0
    price_cols = ["Open", "High", "Low", "Close", "Adj Close"]

    neg_prices = (df[price_cols] < 0).any().any()
    if bool(neg_prices):
        errs.append("Hay precios negativos")

    neg_vol = (df["Volume"] < 0).any()
    if bool(neg_vol):
        errs.append("Hay volumen negativo")

    # 4) NaNs sospechosos
    has_nans = df[price_cols + ["Volume"]].isna().any().any()
    if bool(has_nans):
        errs.append("Hay valores NaN en precios/volumen")

    return errs



def download_one(ticker: str, period: str, interval: str) -> pd.DataFrame:
    df = yf.download(
        ticker,
        period=period,
        interval=interval,
        auto_adjust=False,
        progress=False,
        group_by="column",  # pide agrupar por columnas
    )
    if df.empty:
        raise ValueError(f"Sin datos para {ticker}")
    df = df.reset_index()

    # 🔧 Si viene con columnas multinivel, las aplanamos
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [c[0] if c[0] != "Date" else "Date" for c in df.columns]

    return df



def save_parquet(df: pd.DataFrame, ticker: str):
    path = RAW_DIR / f"{ticker}.parquet"
    df.to_parquet(path, index=False)
    return path

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--tickers", type=str, required=True, help="Lista separada por coma. Ej: NVDA,MSFT")
    parser.add_argument("--period", type=str, default="5y", help="Ej: 1y, 5y, max")
    parser.add_argument("--interval", type=str, default="1d", help="Ej: 1d, 1wk, 1mo")
    args = parser.parse_args()

    tickers = [t.strip().upper() for t in args.tickers.split(",") if t.strip()]
    print(f"Descargando {len(tickers)} tickers: {tickers}")

    ok, bad = [], []
    for t in tickers:
        try:
            df = download_one(t, args.period, args.interval)
            print(f"[{t}] columnas: {list(df.columns)}  tipos: {df.dtypes.to_dict()}")
            errs = validate(df, t)
            if errs:
                print(f"[{t}] ❗ Validaciones con problemas: {', '.join(errs)}")
            path = save_parquet(df, t)
            print(f"[{t}] ✅ {len(df)} filas → {path}")
            ok.append(t)
        except Exception as e:
            print(f"[{t}] ❌ Error: {e}")
            bad.append(t)

    print(f"\nResumen: OK={ok}  |  FALLAS={bad}")
