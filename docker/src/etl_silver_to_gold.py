import os
from datetime import datetime
from pyspark.sql import SparkSession
from dotenv import load_dotenv

load_dotenv()

def transform_breweries_bronze_to_silver(date_str: str):
    """
    Transforma dados da camada bronze para a silver, particionando por ano, mês e dia.
    
    :param date_str: Data de referência no formato 'YYYY-MM-DD'
    """
    # Constrói caminhos
    input_path = f"s3a://datalake/bronze/breweries_{date_str}.json"
    output_path = f"s3a://datalake/silver/breweries/"

    # Inicializa Spark
    spark = SparkSession.builder \
        .appName("Transform Bronze to Silver") \
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("S3_ENDPOINT_URL", "http://localhost:9000")) \
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID")) \
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY")) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    # Lê dados bronze
    df = spark.read.json(input_path)

    # Filtra e seleciona colunas desejadas
    df = df.select("id", "name", "brewery_type", "city", "state", "country")

    # Adiciona colunas de partição
    exec_date = datetime.strptime(date_str, "%Y-%m-%d")
    df = df.withColumn("ano", spark.sql.functions.lit(exec_date.year)) \
           .withColumn("mes", spark.sql.functions.lit(exec_date.month)) \
           .withColumn("dia", spark.sql.functions.lit(exec_date.day))

    # Salva em formato JSON particionado
    df.write.mode("overwrite").partitionBy("ano", "mes", "dia").json(output_path)

    spark.stop()
    print(f"Dados da bronze {input_path} transformados e salvos na silver em {output_path}")


if __name__ == "__main__":
    transform_breweries_bronze_to_silver("2025-06-07")  # Substitua pela data desejada
