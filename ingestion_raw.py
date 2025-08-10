import os
import tempfile
from pathlib import Path
import boto3
from kaggle.api.kaggle_api_extended import KaggleApi

# ---------- CONFIG ----------
DATASET = "rounakbanik/pokemon"
S3_BUCKET = "mybucket-digo"
S3_PREFIX = "raw/kaggle/pokemon-dataset"
# -----------------------------------


def upload_to_s3(file_path: Path, bucket: str, key: str):
    """
    Upstream the local file to the S3 bucket.
    - file_path: caminho local do arquivo
    - bucket: nome do bucket
    - key: path dentro do bucket
    """
    s3 = boto3.client("s3")
    s3.upload_file(str(file_path), bucket, key)
    print(f"Enviado para s3://{bucket}/{key}")

def ingest_kaggle_to_s3(dataset: str, bucket: str, prefix: str):
    # 1. Autenticação no Kaggle
    # A API lê as credenciais do ~/.kaggle/kaggle.json
    api = KaggleApi()
    api.authenticate()
    print("Autenticado no Kaggle! ")

    # 2. Criar diretório temporário para o download
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)

        # 3. Baixar e descompactar
        # Kaggle entrega .zip — unzip=True já extrai
        print(f"⬇ Baixando dataset {dataset} ...")
        api.dataset_download_files(dataset, path=str(tmp_path), unzip=True)

        # 4. Listar todos os arquivos extraídos (pode haver mais de um CSV)
        files = list(tmp_path.glob("**/*"))
        if not files:
            raise FileNotFoundError("Nenhum arquivo encontrado no dataset.")
        print(f"Arquivos extraídos: {[f.name for f in files]}")

        # 5. Enviar cada arquivo para o S3
        for file in files:
            if file.is_file():
                s3_key = f"{prefix}/{file.name}"
                upload_to_s3(file, bucket, s3_key)

        # 6. (Opcional) limpeza automática
        tempfile.TemporaryDirectory() # garante exclusão ao sair do bloco
    print("🎉 Ingestão Kaggle → S3 concluída.")

if __name__ == "__main__":
    ingest_kaggle_to_s3(DATASET, S3_BUCKET, S3_PREFIX)
