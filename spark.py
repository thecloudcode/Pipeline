import os
os.environ['PYSPARK_PYTHON'] = r'C:\Users\badal\AppData\Local\Programs\Python\Python311\python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = r'C:\Users\badal\AppData\Local\Programs\Python\Python311\python.exe'


from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import uuid
import config
from pyspark.sql.types import StringType



def generate_uuid():
    return str(uuid.uuid4())

generate_uuid_udf = F.udf(generate_uuid, StringType())

spark = SparkSession.builder \
    .appName("CassandraSparkConnector") \
    .config("spark.cassandra.connection.host", config.server) \
    .config("spark.cassandra.connection.port", "9042") \
    .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.1.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

campaign_performance_df = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="campaign_performance", keyspace="futurense") \
    .load() \
    .select("campaign_name", "adset_name") \
    .distinct()

campaigns_df = campaign_performance_df \
    .select("campaign_name") \
    .distinct() \
    .withColumn("campaign_id", generate_uuid_udf()) \
    .select("campaign_id", "campaign_name")
campaigns_df.show()

campaigns_df.write \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="campaigns", keyspace="futurense") \
    .mode("append") \
    .save()

adsets_df = campaign_performance_df \
    .select("adset_name") \
    .distinct() \
    .withColumn("adset_id", generate_uuid_udf()) \
    .select("adset_id", "adset_name")
adsets_df.show()

adsets_df.write \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="adsets", keyspace="futurense") \
    .mode("append") \
    .save()

spark.stop()
