"""
RAW -> BRONZE
- Lê CSV/ZIP diretamente do S3 na camada RAW
- Padroniza nomes de colunas (snake_case) e adiciona colunas técnicas
- Escreve Parquet (Snappy) na camada BRONZE, particionado por ingestion_date

Python 3.8.10
"""

import os, io, hashlib, zipfile
from datetime import datetime, timezone
from typing import Dict, Iterable, Tuple

import boto3
import pandas as pd
from slugify import slugify

from utils import (
    setup_logger, utc_now_iso, sha256_bytes, get_batch_id,
    to_snake_cols, read_csv_bytes, detect_csv_delimiter,
    get_s3_client, s3_iter_objects, s3_get_object_bytes, s3_put_parquet
)


# ---- ENV ----
AWS_REGION   = os.getenv("AWS_REGION", "sa-east-1")
S3_RAW_BUCKET = "mybucket-digo"
S3_RAW_PREFIX = "raw/kaggle/pokemon-dataset"

RAW_BUCKET   = os.getenv("RAW_BUCKET", S3_RAW_BUCKET)
RAW_PREFIX   = os.getenv("RAW_PREFIX", S3_RAW_PREFIX)

DL_BUCKET     = os.getenv("DL_BUCKET", RAW_BUCKET)
BRONZE_PREFIX = os.getenv("BRONZE_PREFIX", "bronze/kaggle/pokemon")

RUN_ID   = os.getenv("RUN_ID") or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
BATCH_ID = get_batch_id()  # YYYY_MM_DD_HH (UTC)

# CSV tuning opcional
CSV_DELIM    = os.getenv("CSV_DELIM")      # p.ex. ';'
CSV_ENCODING = os.getenv("CSV_ENCODING")   # p.ex. 'latin1'

# -------------------- client & log --------------------
s3  = boto3.client("s3", region_name=AWS_REGION)
log = setup_logger("raw_to_bronze")

# ----------------- processamento ----------------
def process_csv_bytes(csv_bytes: bytes, source_name: str, run_id: str, ingestion_date: str) -> None:
    kwargs: Dict = {}
    if CSV_DELIM:
        kwargs["sep"] = CSV_DELIM
    else:
        sep = detect_csv_delimiter(csv_bytes[:4096])
        if sep:
            kwargs["sep"] = sep
    if CSV_ENCODING:
        kwargs["encoding"] = CSV_ENCODING

    df = read_csv_bytes(csv_bytes, **kwargs)
    df = to_snake_cols(df)

    file_hash = sha256_bytes(csv_bytes)
    ts_utc = utc_now_iso()

    # colunas técnicas
    df["_ingestion_ts_utc"] = ts_utc
    df["_source_file"]      = source_name
    df["_source_sha256"]    = file_hash
    df["_run_id"]           = run_id
    df["_batch_id"]         = BATCH_ID

    # caminho BRONZE com data e hora (subpartição)
    out_key = (
        f"{BRONZE_PREFIX}/ingestion_date={ingestion_date}/"
        f"batch_id={BATCH_ID}/"
        f"pokemon__{file_hash[:12]}.parquet"
    )
    meta = {
        "layer": "bronze",
        "domain": "kaggle",
        "table": "pokemon",
        "run_id": run_id,
        "batch_id": BATCH_ID,
        "source_file": source_name,
        "source_sha256": file_hash,
    }
    s3_put_parquet(s3, df, DL_BUCKET, out_key, meta)
    log.info("✅ BRONZE: s3://%s/%s", DL_BUCKET, out_key)

def bronze_from_s3_raw() -> None:
    run_id = RUN_ID
    ingestion_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    any_found = False
    log.info("Listando RAW em s3://%s/%s", RAW_BUCKET, RAW_PREFIX)

    for key, _size in s3_iter_objects(s3, RAW_BUCKET, RAW_PREFIX):
        any_found = True
        lower = key.lower()
        blob = s3_get_object_bytes(s3, RAW_BUCKET, key)

        if lower.endswith(".csv"):
            log.info("Processando CSV: s3://%s/%s", RAW_BUCKET, key)
            process_csv_bytes(blob, source_name=key.split("/")[-1], run_id=run_id, ingestion_date=ingestion_date)

        elif lower.endswith(".zip"):
            log.info("Processando ZIP: s3://%s/%s", RAW_BUCKET, key)
            with zipfile.ZipFile(io.BytesIO(blob)) as z:
                for info in z.infolist():
                    name = info.filename
                    if name.endswith("/") or not name.lower().endswith(".csv"):
                        continue
                    process_csv_bytes(z.read(name), source_name=name, run_id=run_id, ingestion_date=ingestion_date)

        else:
            log.info("Ignorado (não é CSV/ZIP): s3://%s/%s", RAW_BUCKET, key)

    if not any_found:
        raise FileNotFoundError(
            f"Nenhum objeto encontrado em s3://{RAW_BUCKET}/{RAW_PREFIX}/ "
            "— verifique RAW_BUCKET/RAW_PREFIX ou permissões."
        )

if __name__ == "__main__":
    bronze_from_s3_raw()
