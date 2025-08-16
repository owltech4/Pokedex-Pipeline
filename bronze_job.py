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

from config import CFG

from utils import (
    setup_logger, utc_now_iso, sha256_bytes, get_batch_id,
    to_snake_cols, read_csv_bytes, detect_csv_delimiter,
    get_s3_client, s3_iter_objects, s3_get_object_bytes, s3_put_parquet,
    exec_sql_file, redshift_table_exists
)


# ---- ENV ----
AWS_REGION   = CFG.aws_region
RAW_BUCKET   = CFG.raw.bucket
RAW_PREFIX   = CFG.raw.prefix
DL_BUCKET     = CFG.bronze.bucket
BRONZE_PREFIX = CFG.bronze.prefix
CSV_DELIM    = CFG.csv_delim
CSV_ENCODING = CFG.csv_encoding

# ---- Execução ----
RUN_ID   = os.getenv("RUN_ID") or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
BATCH_ID = get_batch_id()  # YYYY_MM_DD_HH (UTC)

# ---- Redshift / Spectrum (lidos do CFG.redshift) ----
RS = CFG.redshift
REDSHIFT_REGION     = RS.region
REDSHIFT_DATABASE   = RS.database
REDSHIFT_WORKGROUP  = RS.workgroup      # Serverless (preferível)
REDSHIFT_CLUSTER_ID = RS.cluster_id     # (ou) Cluster provisionado
REDSHIFT_SECRET_ARN = RS.secret_arn
EXTERNAL_SCHEMA     = RS.external_schema     # ex.: "spectrum"
GLUE_DATABASE       = RS.glue_database       # ex.: "dl_catalog"
IAM_ROLE_ARN        = RS.iam_role_arn        # arn:aws:iam::...:role/RedshiftSpectrumRole
SQL_DIR             = RS.sql_dir             # "sql/redshift/externals"
ENABLE_REDSHIFT_DDL = RS.enable_ddls         # True/False


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


    # ---- DDLs do Spectrum (opcional) ----
    if ENABLE_REDSHIFT_DDL:
        if not IAM_ROLE_ARN:
            log.warning("ENABLE_REDSHIFT_DDL=True, mas IAM_ROLE_ARN vazio. Pulando DDLs.")
            return
        if not (REDSHIFT_WORKGROUP or REDSHIFT_CLUSTER_ID):
            log.warning("ENABLE_REDSHIFT_DDL=True, mas faltam REDSHIFT_WORKGROUP/CLUSTER_ID. Pulando DDLs.")
            return

        params = {
            "EXTERNAL_SCHEMA": EXTERNAL_SCHEMA,
            "GLUE_DATABASE": GLUE_DATABASE,
            "IAM_ROLE_ARN": IAM_ROLE_ARN,
            "S3_BRONZE": f"s3://{DL_BUCKET}/{BRONZE_PREFIX}/",
            "INGESTION_DATE": ingestion_date,
            "BATCH_ID": BATCH_ID,
        }
        exec_kwargs = dict(
            database=REDSHIFT_DATABASE,
            workgroup=REDSHIFT_WORKGROUP,
            cluster_id=REDSHIFT_CLUSTER_ID,
            secret_arn=REDSHIFT_SECRET_ARN,
            region_name=REDSHIFT_REGION,
        )

        # 1) schema externo
        exec_sql_file(os.path.join(SQL_DIR, "00_create_external_schema.sql"), params, **exec_kwargs)
        # 2) tabela externa — só cria se não existir
        if not redshift_table_exists(
            EXTERNAL_SCHEMA, "pokemon",
            database=REDSHIFT_DATABASE,
            workgroup=REDSHIFT_WORKGROUP,
            cluster_id=REDSHIFT_CLUSTER_ID,
            secret_arn=REDSHIFT_SECRET_ARN,
            region_name=REDSHIFT_REGION,
        ):
            exec_sql_file(os.path.join(SQL_DIR, "01_create_external_table_pokemon.sql"), params, **exec_kwargs)
            log.info("✅ Tabela externa criada: %s.pokemon", EXTERNAL_SCHEMA)
        else:
            log.info("ℹ️ Tabela externa já existe: %s.pokemon — pulando CREATE.", EXTERNAL_SCHEMA)

        # 3) registra a partição deste batch
        exec_sql_file(os.path.join(SQL_DIR, "02_add_partition.sql"), params, **exec_kwargs)

        log.info("✅ Spectrum atualizado (dt=%s, batch=%s).", ingestion_date, BATCH_ID)
    else:
        log.info("Spectrum desativado (ENABLE_REDSHIFT_DDL=False).")

if __name__ == "__main__":
    bronze_from_s3_raw()
