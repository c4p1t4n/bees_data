import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from airflow.utils.log.logging_mixin import LoggingMixin

logger = LoggingMixin().log

def read_json_from_bronze(spark,file_path) -> SparkSession:
    """
    Lê um arquivo JSON da camada bronze e retorna um DataFrame Spark.
    
    Args:
        file_path (str): Caminho do arquivo JSON na camada bronze.
    
    Returns:
        SparkSession: DataFrame Spark contendo os dados lidos do JSON.
    """
    df = spark.read.json(file_path)
    logger.info(f"Dados lidos do arquivo {file_path} com {df.count()} registros.")
    return df



def clean_data(df):
    """
    Realiza tratamentos gerais:
    - Remove duplicatas
    - Remove linhas com valores nulos nas colunas principais (se existirem)
    - Faz cast de colunas relevantes para StringType
    """
    df = df.dropDuplicates()
    required_cols = ["country", "state", "city"]
    for col in required_cols:
        if col in df.columns:
            df = df.filter(F.col(col).isNotNull())
    return df


def write_to_silver(df, output_path):
    """
    Escreve um DataFrame Spark na camada silver em formato Parquet particionado por localização.
    
    Args:
        df (SparkSession): DataFrame Spark a ser escrito.
        output_path (str): Caminho de saída para salvar os dados na camada silver.
    """
    df.write.mode("overwrite") \
        .partitionBy("country", "state", "city") \
        .parquet(output_path)
    logger.info(f"Dados salvos na camada silver em: {output_path}")



def transform_bronze_to_silver():
    """
    Lê todos os arquivos da camada bronze, transforma e salva na camada silver
    em formato Parquet particionado por localização.
    """
    OUTPUT_PATH = "s3a://datalake/silver/"
    spark = SparkSession.builder \
        .appName("Transform All Bronze to Silver") \
        .getOrCreate()

    df = read_json_from_bronze(spark,"s3a://datalake/bronze/data.json")
    df = clean_data(df)
    write_to_silver(df, OUTPUT_PATH)

    spark.stop()
    logger.info(f"Todos os dados da bronze foram transformados e salvos na silver em: {OUTPUT_PATH}")


if __name__ == "__main__":
    transform_bronze_to_silver()
