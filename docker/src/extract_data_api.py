# src/extract_data_api.py
import requests
import json
import os
from datetime import datetime
import boto3
from botocore.client import Config
from airflow.utils.log.logging_mixin import LoggingMixin

def extract_breweries_to_minio(bucket_name: str, object_key_prefix: str, execution_date: str = None) -> str:
    """
    Extrai dados da API Open Brewery DB e envia para o MinIO (S3).

    Args:
        bucket_name (str): Nome do bucket no MinIO.
        object_key_prefix (str): Prefixo do caminho no S3 (ex: bronze/breweries).
        execution_date (str): Data da execução (formato YYYY-MM-DD).

    Returns:
        str: Caminho (key) do arquivo salvo no S3.
    """
    logger = LoggingMixin().log

    if not execution_date:
        execution_date = datetime.today().strftime("%Y-%m-%d")

    logger.info(f"Iniciando extração da API Open Brewery DB - Data: {execution_date}")

    all_breweries = []
    page = 1
    per_page = 50
    url = f"https://api.openbrewerydb.org/v1/breweries?page={page}&per_page={per_page}"
    response = requests.get(url)

    if response.status_code != 200:
        logger.error(f"Erro ao acessar API: {response.status_code} - {response.text}")
        raise Exception(f"Erro ao acessar API: {response.status_code} - {response.text}")

    breweries = response.json()
    logger.info(f"{len(breweries)} registros obtidos da API")

    json_data = json.dumps(breweries, ensure_ascii=False, indent=2)
    object_key = f"{object_key_prefix}/breweries_{execution_date}.json"

    s3 = boto3.client(
        "s3",
        endpoint_url=os.getenv("S3_ENDPOINT_URL", "http://localhost:9000"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "H1Cv8fzwXljv2Vl3RO79"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "nQCkdSU2GHSnjC36shOijpSZuIZxGc8BNsHRBU0A"),
        config=Config(signature_version="s3v4"),
        region_name="us-east-1"
    )

    s3.put_object(Bucket=bucket_name, Key=object_key, Body=json_data.encode("utf-8"))

    logger.info(f"Upload finalizado com sucesso: s3://{bucket_name}/{object_key}")
    return object_key
