# config.py — parâmetros do Pokemon Pipeline (Python 3.8.10)
# Estratégia: pode usar env vars (modo atual) OU valores estáticos definidos abaixo.
from dataclasses import dataclass
from typing import Optional
import os

# --------- toggle do modo de configuração ---------
# False = usa valores estáticos abaixo (sem variáveis de ambiente)
# True  = lê tudo de os.getenv(...) como era antes
USE_ENV = False

def _env_str(name: str, default: Optional[str] = None) -> Optional[str]:
    return os.getenv(name, default)

def _env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "t", "yes", "y", "on")

@dataclass
class S3Location:
    bucket: str
    prefix: str

@dataclass
class RedshiftConfig:
    region: str
    database: str
    workgroup: Optional[str] = None     # Serverless
    cluster_id: Optional[str] = None    # Provisionado
    secret_arn: Optional[str] = None    # deixe None para Data API via IAM (sem Secret)
    external_schema: str = "spectrum"
    glue_database: str = "dl_catalog"
    iam_role_arn: str = "arn:aws:iam::920373029355:role/RedshiftSpectrumRole"
    sql_dir: str = "sql/redshift/externals"
    enable_ddls: bool = True            # cria schema/tabela/partição automaticamente

@dataclass
class Config:
    aws_region: str
    raw: S3Location
    bronze: S3Location
    redshift: RedshiftConfig
    csv_delim: Optional[str] = None
    csv_encoding: Optional[str] = None

    # ---------- MODO 1: via variáveis de ambiente (comportamento original) ----------
    @classmethod
    def from_env(cls) -> "Config":
        # Região
        aws_region = _env_str("AWS_REGION", "sa-east-1")

        # S3 RAW/BRONZE (mesmo bucket, pastas diferentes — seu padrão)
        raw_bucket = _env_str("RAW_BUCKET", "mybucket-digo")
        raw_prefix = _env_str("RAW_PREFIX", "raw/kaggle/pokemon-dataset")
        bronze_bucket = _env_str("DL_BUCKET", raw_bucket)
        bronze_prefix = _env_str("BRONZE_PREFIX", "bronze/kaggle/pokemon")

        # Redshift
        rs_region  = _env_str("REDSHIFT_REGION", aws_region)
        rs_db      = _env_str("REDSHIFT_DATABASE", "dev")
        rs_wg      = _env_str("REDSHIFT_WORKGROUP")      # Serverless
        rs_cluster = _env_str("REDSHIFT_CLUSTER_ID")     # Provisionado
        rs_secret  = _env_str("REDSHIFT_SECRET_ARN")
        rs_schema  = _env_str("EXTERNAL_SCHEMA", "spectrum")
        glue_db    = _env_str("GLUE_DATABASE", "dl_catalog")
        iam_role   = _env_str("REDSHIFT_IAM_ROLE_ARN", "arn:aws:iam::920373029355:role/RedshiftSpectrumRole")
        sql_dir    = _env_str("SQL_DIR", "sql/redshift/externals")
        enable     = _env_bool("ENABLE_REDSHIFT_DDL", True)

        # CSV (opcional)
        csv_delim    = _env_str("CSV_DELIM")
        csv_encoding = _env_str("CSV_ENCODING")

        return cls(
            aws_region=aws_region,
            raw=S3Location(raw_bucket, raw_prefix),
            bronze=S3Location(bronze_bucket, bronze_prefix),
            redshift=RedshiftConfig(
                region=rs_region,
                database=rs_db,
                workgroup=rs_wg,
                cluster_id=rs_cluster,
                secret_arn=rs_secret,
                external_schema=rs_schema,
                glue_database=glue_db,
                iam_role_arn=iam_role,
                sql_dir=sql_dir,
                enable_ddls=enable,
            ),
            csv_delim=csv_delim,
            csv_encoding=csv_encoding,
        )

    # ---------- MODO 2: valores estáticos (sem env vars) ----------
    @classmethod
    def from_static(cls) -> "Config":
        # Ajuste aqui os seus valores fixos
        aws_region = "sa-east-1"

        raw_bucket = "mybucket-digo"
        raw_prefix = "raw/kaggle/pokemon-dataset"

        bronze_bucket = "mybucket-digo"          # mesmo bucket por padrão
        bronze_prefix = "bronze/kaggle/pokemon"

        redshift_cfg = RedshiftConfig(
            region="sa-east-1",
            database="dev",
            workgroup="default-workgroup",
            cluster_id=None,
            secret_arn=None,                     # None -> Data API via IAM (sem Secret)
            external_schema="spectrum",
            glue_database="dl_catalog",
            iam_role_arn="arn:aws:iam::920373029355:role/RedshiftSpectrumRole",
            sql_dir="sql/redshift/externals",
            enable_ddls=True,
        )

        # CSV (opcional)
        csv_delim = None         # ex.: ";"
        csv_encoding = None      # ex.: "latin1"

        return cls(
            aws_region=aws_region,
            raw=S3Location(raw_bucket, raw_prefix),
            bronze=S3Location(bronze_bucket, bronze_prefix),
            redshift=redshift_cfg,
            csv_delim=csv_delim,
            csv_encoding=csv_encoding,
        )

# --------- Instância global pronta para importar ---------
CFG = Config.from_env() if USE_ENV else Config.from_static()

# (opcional) debug rápido
if __name__ == "__main__":
    from pprint import pprint
    pprint(CFG)
