import requests
import json
import os
from datetime import datetime
import boto3
from botocore.client import Config
from airflow.utils.log.logging_mixin import LoggingMixin


logger = LoggingMixin().log
def fetch_breweries_from_api() -> str:
    logger.info(f"Iniciando extração da API Open Brewery DB - Data: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    all_breweries = []
    page = 1
    per_page = 50

    while True:
        url = f"https://api.openbrewerydb.org/v1/breweries?page={page}&per_page={per_page}"
        response = requests.get(url)

        if response.status_code != 200:
            logger.error(f"Erro ao acessar API: {response.status_code} - {response.text}")
            raise Exception(f"Erro ao acessar API: {response.status_code} - {response.text}")

        breweries = response.json()
        logger.info(f"Página {page}: {len(breweries)} registros obtidos")

        if not breweries:
            break

        all_breweries.extend(breweries)
        page += 1

    logger.info(f"Total de registros extraídos: {len(all_breweries)}")

    json_data = "\n".join(json.dumps(brewery, ensure_ascii=False) for brewery in all_breweries)
    return json_data


def put_object_to_s3(bucket_name: str, object_key: str, data: str):
    logger.info(f"Enviando dados para o bucket S3: {bucket_name}, chave: {object_key}")
    s3_client = boto3.client(
        's3',
        endpoint_url=os.getenv("S3_ENDPOINT_URL", "http://localhost:9000"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID","airflowuser"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY","airflowpass123"),
        config=Config(signature_version='s3v4')
    )
    s3_client.put_object(Bucket=bucket_name, Key=object_key, Body=data)



def main():
    bucket_name = os.getenv("BRONZE_BUCKET", "datalake")
    object_key_prefix = "bronze/data.json"

    logger.info("Iniciando extração de dados da API Open Brewery DB")
    json_data = fetch_breweries_from_api()
    logger.info("Dados extraídos com sucesso, enviando para o S3")
    logger.info(f"Bucket: {bucket_name}, Chave: {object_key_prefix}")
    put_object_to_s3(
        bucket_name=bucket_name,
        object_key=object_key_prefix,
        data=json_data
    )
    

if __name__ == "__main__":
    main()
    print("Dados extraídos e salvos com sucesso!")