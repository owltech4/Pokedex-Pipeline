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

# ---- ENV ----
AWS_REGION   = os.getenv("AWS_REGION", "sa-east-1")
S3_RAW_BUCKET = "mybucket-digo"
S3_RAW_PREFIX = "raw/kaggle/pokemon-dataset"

RAW_BUCKET   = os.getenv("RAW_BUCKET", S3_RAW_BUCKET)
RAW_PREFIX   = os.getenv("RAW_PREFIX", S3_RAW_PREFIX)

DL_BUCKET     = os.getenv("DL_BUCKET", RAW_BUCKET)
BRONZE_PREFIX = os.getenv("BRONZE_PREFIX", "bronze/kaggle/pokemon")

RUN_ID = os.getenv("RUN_ID")

# -------------------- client --------------------
s3 = boto3.client("s3", region_name=AWS_REGION)

# -------------------- utils --------------------
def sha256_bytes(data: bytes) -> str:
    h = hashlib.sha256(); h.update(data); return h.hexdigest()

def to_snake_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [slugify(c, separator="_") for c in df.columns]
    return df

def iter_raw_objects(bucket: str, prefix: str) -> Iterable[Tuple[str, int]]:
    """Lista objetos em s3://bucket/prefix (key, size)."""
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Size"] == 0:
                continue
            yield obj["Key"], obj["Size"]

def get_object_bytes(bucket: str, key: str) -> bytes:
    resp = s3.get_object(Bucket=bucket, Key=key)
    return resp["Body"].read()

def put_parquet_to_s3(df: pd.DataFrame, bucket: str, key: str, metadata: Dict[str, str]) -> None:
    import pyarrow as pa, pyarrow.parquet as pq
    buf = io.BytesIO()
    pq.write_table(pa.Table.from_pandas(df, preserve_index=False), buf, compression="snappy")
    buf.seek(0)
    s3.put_object(Bucket=bucket, Key=key, Body=buf.read(), Metadata={k:str(v) for k,v in metadata.items()})

# ----------------- processamento ----------------
def process_csv_bytes(csv_bytes: bytes, source_name: str, run_id: str, ingestion_date: str) -> None:
    df = pd.read_csv(io.BytesIO(csv_bytes))
    df = to_snake_cols(df)

    file_hash = sha256_bytes(csv_bytes)
    ts_utc = datetime.now(timezone.utc).isoformat()

    # colunas técnicas
    df["_ingestion_ts_utc"] = ts_utc
    df["_source_file"]      = source_name
    df["_source_sha256"]    = file_hash
    df["_run_id"]           = run_id

    # grava no bronze particionado por data de ingestão
    out_key = f"{BRONZE_PREFIX}/ingestion_date={ingestion_date}/pokemon__{file_hash[:12]}.parquet"
    meta = {
        "layer": "bronze",
        "domain": "kaggle",
        "table": "pokemon",
        "run_id": run_id,
        "source_file": source_name,
        "source_sha256": file_hash,
    }
    put_parquet_to_s3(df, DL_BUCKET, out_key, meta)
    print(f"✅ BRONZE: s3://{DL_BUCKET}/{out_key}")

def bronze_from_s3_raw() -> None:
    run_id = RUN_ID or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    ingestion_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    any_found = False
    for key, _size in iter_raw_objects(RAW_BUCKET, RAW_PREFIX):
        any_found = True
        lower = key.lower()
        blob = get_object_bytes(RAW_BUCKET, key)

        if lower.endswith(".csv"):
            process_csv_bytes(blob, source_name=key.split("/")[-1], run_id=run_id, ingestion_date=ingestion_date)

        elif lower.endswith(".zip"):
            with zipfile.ZipFile(io.BytesIO(blob)) as z:
                for info in z.infolist():
                    name = info.filename
                    if name.endswith("/") or not name.lower().endswith(".csv"):
                        continue
                    process_csv_bytes(z.read(name), source_name=name, run_id=run_id, ingestion_date=ingestion_date)

        else:
            print(f"ℹ️ Ignorado (não é CSV/ZIP): s3://{RAW_BUCKET}/{key}")

    if not any_found:
        raise FileNotFoundError(
            f"Nenhum objeto encontrado em s3://{RAW_BUCKET}/{RAW_PREFIX}/ "
            "— verifique RAW_BUCKET/RAW_PREFIX ou permissões."
        )

if __name__ == "__main__":
    bronze_from_s3_raw()
