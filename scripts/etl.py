from pyspark.sql.functions import col, split
import sys

if "/opt/spark/app" not in sys.path:
    sys.path.append("/opt/spark/app")

from config.spark_config import get_spark_session

spark = get_spark_session()


# Extract (Load data into pyspark dataframe)

airbnb_listing_df = spark.read\
    .format('csv')\
    .options(header=True)\
    .options(inferSchema=True)\
    .load("hdfs://namenode:8020/user/hadoop/input/listings_aus.csv")

# Transform (Transform data based on the requirrements)

""" Based on our business use, we only require this columns out of 75 columns available in dataset """

req_df = airbnb_listing_df.select(
    'id',
    'name',
    col('host_id').cast('double'),
    'host_name',
    col('neighbourhood_cleansed').alias('neighbourhood'),
    'room_type',
    'price'
)

""" As price columns has values like '$214.00' we will be transforming it to '214.00' and cast datatype to double """

price_transformed = req_df.withColumn(
    'price', split(
        col('price'), '\\$'
    ).getItem(1).cast('double')
)

""" once we get the required data, now we will drop the null values from the dataframe """

final_df = price_transformed.dropna()

# Load (loading data into postgresql database table)

final_df.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://postgres-db:5432/airbnb") \
    .option("dbtable", "airbnb_listings") \
    .option("user", "user") \
    .option("password", "password") \
    .mode("overwrite") \
    .save()
