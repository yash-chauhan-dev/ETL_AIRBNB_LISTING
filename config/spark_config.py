from pyspark.sql import SparkSession


def get_spark_session():

    spark = SparkSession.builder \
        .appName("AirbnbETL") \
        .master("spark://spark-master:7077") \
        .config("spark.jars", "hdfs://namenode:8020/user/hadoop/input/postgresql-42.2.20.jar") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020") \
        .config("spark.sql.debug.maxToStringFields", 100) \
        .getOrCreate()

    return spark
