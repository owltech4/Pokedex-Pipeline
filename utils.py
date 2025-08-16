"""
utils.py - utilitários reutilizáveis para jobs de ingestão (Python 3.8.10)
Este módulo centraliza:
- logging
- datas e hash
- normalização de colunas
- helpers de CSV em memória
- helpers de S3 (listar/ler/escrever) e escrever Parquet direto no S3
"""

import csv
import io
import time
from pathlib import Path
import hashlib
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Optional, Sequence, Tuple

import boto3
import pandas as pd
from slugify import slugify


# --------------------- Batch helpers ----------------
def get_batch_id(dt: Optional[datetime] = None, tz=timezone.utc) -> str:
    """
    Retorna o batch_id no formato YYYY_MM_DD_HH.
    - dt: datetime de referência; se None, usa o now() no timezone informado.
    - tz: timezone desejado (default: UTC). Use timezone.utc ou um tz do pendulum.
    """
    dt = (dt or datetime.now(tz)).astimezone(tz)
    return dt.strftime("%Y_%m_%d_%H")

# --------------------- Logging ---------------------
def setup_logger(name: str = "ingestion", level: int = logging.INFO) -> logging.Logger:
    """
    Logger simples para jobs de dados.
    """
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s - %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    logger.setLevel(level)
    return logger


# ---------------- Datas e hash ---------------------
def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def sha256_bytes(data: bytes) -> str:
    h = hashlib.sha256()
    h.update(data)
    return h.hexdigest()


# --------------- Pandas / CSV helpers --------------
def to_snake_cols(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converte nomes de colunas para snake_case seguro (minúsculo, sem acentos).
    """
    df = df.copy()
    df.columns = [slugify(str(c), separator="_") for c in df.columns]
    return df

def detect_csv_delimiter(sample: bytes) -> Optional[str]:
    """
    Tenta detectar o delimitador a partir de um sample (bytes).
    Retorna None se não conseguir — o caller pode cair no padrão do pandas (',').
    """
    try:
        text = sample.decode("utf-8", errors="ignore")
        dialect = csv.Sniffer().sniff(text[:4096], delimiters=[",", ";", "\t", "|"])
        return dialect.delimiter
    except Exception:
        return None

def read_csv_bytes(data: bytes, **kwargs) -> pd.DataFrame:
    """
    Lê CSV a partir de bytes. Você pode passar kwargs como sep, encoding etc.
    Ex.: read_csv_bytes(blob, sep=';', encoding='latin1')
    """
    return pd.read_csv(io.BytesIO(data), **kwargs)


# -------------------- S3 helpers -------------------

def get_s3_client(region_name: Optional[str] = None):
    return boto3.client("s3", region_name=region_name)

def s3_iter_objects(s3_client, bucket: str, prefix: str) -> Iterable[Tuple[str, int]]:
    """
    Itera objetos em s3://bucket/prefix/ retornando (key, size).
    Ignora 'pastas' com size=0.
    """
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            size = obj.get("Size", 0)
            if size == 0:
                continue
            yield obj["Key"], size

def s3_get_object_bytes(s3_client, bucket: str, key: str) -> bytes:
    resp = s3_client.get_object(Bucket=bucket, Key=key)
    return resp["Body"].read()

def s3_put_bytes(s3_client, bucket: str, key: str, data: bytes, metadata: Optional[Dict[str, str]] = None) -> None:
    s3_client.put_object(Bucket=bucket, Key=key, Body=data, Metadata={k: str(v) for k, v in (metadata or {}).items()})

def s3_put_parquet(s3_client, df: pd.DataFrame, bucket: str, key: str, metadata: Optional[Dict[str, str]] = None) -> None:
    """
    Converte DataFrame -> Parquet (Snappy) em memória e sobe para o S3.
    """
    import pyarrow as pa  # lazy import
    import pyarrow.parquet as pq

    buf = io.BytesIO()
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, buf, compression="snappy")
    buf.seek(0)
    s3_put_bytes(s3_client, bucket, key, buf.read(), metadata)

def ensure_bucket_exists(s3_client, bucket: str, region: Optional[str] = None) -> None:
    """
    Útil em DEV. Em produção, prefira IaC (Terraform/CloudFormation).
    Cria o bucket se não existir.
    """
    import botocore
    try:
        s3_client.head_bucket(Bucket=bucket)
    except botocore.exceptions.ClientError as e:
        code = int(e.response.get("Error", {}).get("Code", 0))
        if code == 404:
            if (region or "").lower() in ("", "us-east-1"):
                s3_client.create_bucket(Bucket=bucket)
            else:
                s3_client.create_bucket(
                    Bucket=bucket,
                    CreateBucketConfiguration={"LocationConstraint": region}
                )
        else:
            raise

#---------- Redshift ---------------
def redshift_execute_sql(
    sql: str,
    database: str,
    workgroup: Optional[str] = None,          # Serverless
    cluster_id: Optional[str] = None,         # Provisionado
    secret_arn: Optional[str] = None,         # pode ficar None (IAM sem Secret)
    region_name: Optional[str] = None,
    poll: bool = True,
    timeout_s: int = 600,
) -> str:
    """
    Executa um SQL via Redshift Data API e (opcionalmente) aguarda terminar.
    Retorna o statement id.
    """
    if not (workgroup or cluster_id):
        raise ValueError("Informe workgroup (Serverless) ou cluster_id (provisionado).")
    client = boto3.client("redshift-data", region_name=region_name)
    kwargs = {
        "Database": database,
        "Sql": sql,
        "WithEvent": True,
    }
    if secret_arn:                 # <-- só inclui se houver
        kwargs["SecretArn"] = secret_arn
    if workgroup:
        kwargs["WorkgroupName"] = workgroup
    if cluster_id:
        kwargs["ClusterIdentifier"] = cluster_id

    resp = client.execute_statement(**kwargs)
    stmt_id = resp["Id"]

    if not poll:
        return stmt_id

    # espera concluir
    start = time.time()
    while True:
        desc = client.describe_statement(Id=stmt_id)
        status = desc["Status"]
        if status in ("FINISHED", "FAILED", "ABORTED"):
            if status != "FINISHED":
                raise RuntimeError(f"Redshift SQL falhou: {desc.get('Error', 'unknown')}")
            return stmt_id
        if time.time() - start > timeout_s:
            raise TimeoutError(f"Timeout aguardando statement {stmt_id}")
        time.sleep(2)

def render_sql_template(template_text: str, params: Dict[str, Any]) -> str:
    """
    Renderiza templates .sql usando Python format: {PLACEHOLDER}.
    """
    return template_text.format(**params)

def exec_sql_file(
    file_path: str,
    params: Dict[str, Any],
    database: str,
    workgroup: Optional[str] = None,
    cluster_id: Optional[str] = None,
    secret_arn: Optional[str] = None,
    region_name: Optional[str] = None,
) -> None:
    """
    Lê um .sql, aplica params e executa via Data API.
    Suporta múltiplos statements separados por ';'.
    """
    sql_raw = Path(file_path).read_text(encoding="utf-8")
    sql = render_sql_template(sql_raw, params)

    # split simples; se tiver ponto-e-vírgula dentro de string, ajuste conforme necessário
    statements = [s.strip() for s in sql.split(";") if s.strip()]
    for stmt in statements:
        redshift_execute_sql(
            sql=stmt,
            database=database,
            workgroup=workgroup,
            cluster_id=cluster_id,
            secret_arn=secret_arn,
            region_name=region_name,
            poll=True,
        )
def redshift_table_exists(
    schema: str,
    table: str,
    database: str,
    workgroup: Optional[str] = None,
    cluster_id: Optional[str] = None,
    secret_arn: Optional[str] = None,
    region_name: Optional[str] = None,
    timeout_s: int = 60,
) -> bool:
    client = boto3.client("redshift-data", region_name=region_name)
    sql = f"""
    select 1
    from svv_external_tables
    where schemaname = '{schema.lower()}'
      and tablename  = '{table.lower()}'
    limit 1
    """
    kwargs = {
        "Database": database,
        "Sql": sql,
        "WithEvent": False,
    }
    if secret_arn:
        kwargs["SecretArn"] = secret_arn
    if workgroup:
        kwargs["WorkgroupName"] = workgroup
    if cluster_id:
        kwargs["ClusterIdentifier"] = cluster_id

    resp = client.execute_statement(**kwargs)
    stmt_id = resp["Id"]

    # poll simples
    start = time.time()
    while True:
        desc = client.describe_statement(Id=stmt_id)
        st = desc["Status"]
        if st in ("FAILED", "ABORTED"):
            raise RuntimeError(f"Falha ao checar tabela externa: {desc.get('Error','unknown')}")
        if st == "FINISHED":
            break
        if time.time() - start > timeout_s:
            raise TimeoutError("Timeout checando existência da tabela externa")
        time.sleep(0.5)

    res = client.get_statement_result(Id=stmt_id)
    return len(res.get("Records", [])) > 0
