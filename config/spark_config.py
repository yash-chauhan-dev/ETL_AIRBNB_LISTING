from pyspark.sql import SparkSession


def get_spark_session():

    spark = SparkSession.builder \
        .appName("AirbnbETL") \
        .getOrCreate()

    return spark
