"""
SILVER (BRONZE -> SILVER no mesmo bucket)
- Lê Parquet do BRONZE no S3
- Limpa, tipa, deduplica e cria a ponte de abilities
- Escreve Parquet (Snappy) em SILVER particionado por dt

Python 3.8.10
"""

import os
import io
import ast
from datetime import datetime, timezone
from typing import Dict, Optional

import pandas as pd
import pyarrow.dataset as ds  # pip install pyarrow (e s3fs para S3)
from utils import (
    setup_logger, utc_now_iso, get_batch_id,
    get_s3_client, s3_put_parquet
)

# -------------------- ENV --------------------
AWS_REGION     = os.getenv("AWS_REGION", "sa-east-1")

S3_BUCKET      = os.getenv("DL_BUCKET", "mybucket-digo")              # mesmo bucket
BRONZE_PREFIX  = os.getenv("BRONZE_PREFIX", "bronze/kaggle/pokemon")  # de onde ler
SILVER_PREFIX  = os.getenv("SILVER_PREFIX", "silver/kaggle/pokemon")  # para onde escrever

# Filtros opcionais para ler só um recorte do BRONZE
# Ex.: BRONZE_FILTER_INGESTION_DATE=2025-08-11
BRONZE_FILTER_INGESTION_DATE = os.getenv("BRONZE_FILTER_INGESTION_DATE")  # YYYY-MM-DD
BRONZE_FILTER_BATCH_ID       = os.getenv("BRONZE_FILTER_BATCH_ID")        # YYYY_MM_DD_HH

RUN_ID    = os.getenv("RUN_ID") or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
BATCH_ID  = get_batch_id()  # YYYY_MM_DD_HH (UTC)
DT_TODAY  = datetime.now(timezone.utc).strftime("%Y-%m-%d")  # partição de saída em SILVER

# -------------------- client & log --------------------
s3  = get_s3_client(AWS_REGION)
log = setup_logger("bronze_to_silver")

# -------------------- leitura do BRONZE --------------------
def make_bronze_uri() -> str:
    base = f"s3://{S3_BUCKET}/{BRONZE_PREFIX}/"
    # aplica filtros de partição se fornecidos
    if BRONZE_FILTER_INGESTION_DATE:
        base += f"ingestion_date={BRONZE_FILTER_INGESTION_DATE}/"
    if BRONZE_FILTER_BATCH_ID:
        base += f"batch_id={BRONZE_FILTER_BATCH_ID}/"
    return base

def load_bronze_df() -> pd.DataFrame:
    uri = make_bronze_uri()
    log.info("Lendo BRONZE: %s", uri)
    dataset = ds.dataset(uri, format="parquet")  # pyarrow lê direto do S3
    table = dataset.to_table()
    df = table.to_pandas()
    if df.empty:
        raise FileNotFoundError(f"Nenhum dado encontrado em {uri}")
    return df

# -------------------- transformações --------------------
def parse_abilities_column(df: pd.DataFrame) -> pd.DataFrame:
    if "abilities" not in df.columns:
        return df
    out = df.copy()

    # Muitos datasets Pokémon trazem abilities como string: "['Overgrow', 'Chlorophyll']"
    def parse_abilities(x):
        try:
            return ast.literal_eval(x) if isinstance(x, str) else None
        except Exception:
            return None

    out["abilities_list"] = out["abilities"].apply(parse_abilities)
    # mantém uma coluna string simples no main:
    out["abilities"] = out["abilities_list"].apply(lambda v: ",".join(v) if isinstance(v, list) else None)
    return out

def clean_and_type(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    # Corrigir typo conhecido
    if "classfication" in out.columns and "classification" not in out.columns:
        out = out.rename(columns={"classfication": "classification"})

    # Tipagens explícitas
    int_cols = [c for c in [
        "pokedex_number", "attack", "defense", "sp_attack", "sp_defense", "speed",
        "hp", "experience_growth", "capture_rate", "generation", "is_legendary"
    ] if c in out.columns]
    for c in int_cols:
        out[c] = pd.to_numeric(out[c], errors="coerce").astype("Int64")

    float_cols = [c for c in out.columns if c.startswith("against_")] + \
                 [c for c in ["height_m", "weight_kg", "percentage_male"] if c in out.columns]
    for c in float_cols:
        out[c] = pd.to_numeric(out[c], errors="coerce")

    str_cols = [c for c in ["name", "japanese_name", "type1", "type2", "classification"] if c in out.columns]
    for c in str_cols:
        out[c] = out[c].astype("string").str.strip()

    # abilities -> lista + string
    out = parse_abilities_column(out)

    # is_legendary -> bool
    if "is_legendary" in out.columns:
        out["is_legendary"] = out["is_legendary"].astype("Int64").apply(
            lambda v: None if pd.isna(v) else bool(v)
        )

    # Dedup por BK (pokedex_number), mantendo o mais recente pela ingestion_ts
    if "pokedex_number" in out.columns and "_ingestion_ts_utc" in out.columns:
        out = (
            out.sort_values("_ingestion_ts_utc")
               .drop_duplicates(subset=["pokedex_number"], keep="last")
        )

    # Colunas técnicas de silver
    out["_silver_ts_utc"] = utc_now_iso()
    out["_batch_id"]      = BATCH_ID

    return out

# -------------------- escrita no SILVER --------------------
def write_silver_main(df: pd.DataFrame) -> None:
    key = f"{SILVER_PREFIX}/dt={DT_TODAY}/pokemon.parquet"
    meta = {
        "layer": "silver",
        "table": "pokemon",
        "dt": DT_TODAY,
        "run_id": RUN_ID,
        "batch_id": BATCH_ID,
    }
    s3_put_parquet(df, S3_BUCKET, key, meta)  # s3_put_parquet usa client interno
    log.info("✅ SILVER main: s3://%s/%s", S3_BUCKET, key)

def write_silver_ability_bridge(df: pd.DataFrame) -> None:
    if "pokedex_number" not in df.columns or "abilities_list" not in df.columns:
        log.info("Sem colunas para ponte de abilities — nada a escrever.")
        return
    exploded = df[["pokedex_number", "abilities_list"]].explode("abilities_list").dropna()
    exploded = exploded.rename(columns={"abilities_list": "ability"})
    key = f"{SILVER_PREFIX}/dt={DT_TODAY}/pokemon_ability_bridge.parquet"
    meta = {
        "layer": "silver",
        "table": "pokemon_ability_bridge",
        "dt": DT_TODAY,
        "run_id": RUN_ID,
        "batch_id": BATCH_ID,
    }
    s3_put_parquet(s3, exploded, S3_BUCKET, key, meta)
    log.info("✅ SILVER bridge: s3://%s/%s", S3_BUCKET, key)

# -------------------- main --------------------
def run() -> None:
    bronze_df = load_bronze_df()
    silver_df = clean_and_type(bronze_df)
    write_silver_main(silver_df)
    write_silver_ability_bridge(silver_df)

if __name__ == "__main__":
    run()
