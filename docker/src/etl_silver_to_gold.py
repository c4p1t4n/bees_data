import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from airflow.utils.log.logging_mixin import LoggingMixin
from datetime import datetime
logger = LoggingMixin().log


def read_from_silver(spark, silver_path):
    """
    Lê os dados da camada silver em formato Parquet.
    
    Args:
        spark (SparkSession): Sessão Spark ativa.
        silver_path (str): Caminho para os dados da camada silver.
    
    Returns:
        DataFrame: Dados lidos da camada silver.
    """
    df = spark.read.parquet(silver_path)
    logger.info(f"Leitura da camada silver realizada com sucesso: {silver_path}")
    return df


def aggregate_brewery_data(df):
    """
    Agrega os dados de breweries por tipo e estado.
    
    Args:
        df (DataFrame): Dados da camada silver.
    
    Returns:
        DataFrame: Dados agregados prontos para a camada gold.
    """
    df_gold = df.groupBy("brewery_type", "state") \
                .agg(F.count("*").alias("brewery_count"))
    logger.info("Agregação realizada com sucesso.")
    return df_gold


def write_to_gold(df, execution_date, gold_path):
    """
    Escreve os dados agregados na camada gold particionada por data de execução.
    
    Args:
        df (DataFrame): DataFrame com dados agregados.
        execution_date (str): Data da execução no formato YYYY-MM-DD.
        gold_path (str): Caminho base para salvar os dados da camada gold.
    """
    output_path = os.path.join(gold_path, f"execution_date={execution_date}")
    df.write.mode("overwrite").parquet(output_path)
    logger.info(f"Dados escritos na camada gold: {output_path}")


def transform_silver_to_gold(execution_date: str):
    """
    Orquestra a transformação da silver para gold.
    
    Args:
        execution_date (str): Data da execução no formato YYYY-MM-DD.
    """
    silver_path = "s3a://datalake/silver/"
    gold_path = "s3a://datalake/gold/"

    spark = SparkSession.builder \
        .appName("Transform Silver to Gold") \
        .getOrCreate()

    df_silver = read_from_silver(spark, silver_path)
    df_gold = aggregate_brewery_data(df_silver)
    df_gold.show(n=5, truncate=False)  # Exibe para validação
    write_to_gold(df_gold, execution_date, gold_path)
    spark.stop()
    logger.info("Pipeline silver to gold finalizado com sucesso.")




if __name__ == "__main__":
    today = datetime.today().strftime("%Y-%m-%d")
    transform_silver_to_gold(today)