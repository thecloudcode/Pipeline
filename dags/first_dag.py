try:

    from datetime import timedelta
    from airflow import DAG
    from airflow.operators.python_operator import PythonOperator
    from datetime import datetime
    from kafka import KafkaConsumer
    import json
    from cassandra.cluster import Cluster
    from cassandra.auth import PlainTextAuthProvider
    import config

    print("All Dag modules are ok ......")
except Exception as e:
    print("Error  {} ".format(e))

def insert_into_cassandra(data):
    # Connect to the Cassandra cluster
    cluster = Cluster([config.server], port=9042)
    session = cluster.connect('futurense')

    # Create a prepared statement
    query = """
    INSERT INTO campaign_performance (campaign_id, dates, campaign_name, campaign_start_date, creative_name, total_spent, impressions, clicks, click_through_rate, leads, campaign_platform, adset_name)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    prepared = session.prepare(query)

    # Execute the statement with the data
    session.execute(prepared, (
        data['campaign_id'],
        data['dates'],
        data['campaign_name'],
        data['campaign_start_date'],
        data['creative_name'],
        data['total_spent'],
        data['impressions'],
        data['clicks'],
        data['click_through_rate'],
        data['leads'],
        data['campaign_platform'],
        data['adset_name']
    ))

    session.shutdown()
    cluster.shutdown()

def execute():
    kafka_server = [config.server+":9092"]  # Ensure you have the correct port
    topic = "test_topic"

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=kafka_server,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset="latest",
    )

    campaign_id = 0
    for message in consumer:
        print(message)
        print(message.value)
        data = message.value
        data['campaign_id'] = campaign_id
        campaign_id += 1
        insert_into_cassandra(data)
        break  # Consume only one message

with DAG(
        dag_id="first_dag",
        schedule_interval="@daily",
        default_args={
            "owner": "airflow",
            "retries": 1,
            "retry_delay": timedelta(minutes=5),
            "start_date": datetime(2024, 8, 3),
        },
        catchup=False) as dag:

    execute_task = PythonOperator(
        task_id="execute",
        python_callable=execute,
    )

    execute_task

#
# try:
#     from datetime import timedelta, datetime
#     from airflow import DAG
#     from airflow.operators.python_operator import PythonOperator
#     from kafka import KafkaConsumer
#     import json
#     from cassandra.cluster import Cluster
#     from cassandra.auth import PlainTextAuthProvider
#     from pyspark.sql import SparkSession
#     import config
#
#     print("All Dag modules are ok ......")
# except Exception as e:
#     print(f"Error: {e}")
#
#
# def create_spark_session():
#     spark = SparkSession.builder \
#         .appName("AirflowSparkIntegration") \
#         .config("spark.cassandra.connection.host", config.server) \
#         .config("spark.cassandra.connection.port", "9042") \
#         .getOrCreate()
#     return spark
#
#
# def insert_into_cassandra(data, spark):
#     # Convert data to DataFrame
#     df = spark.createDataFrame([data])
#
#     # Write data to Cassandra
#     df.write \
#         .format("org.apache.spark.sql.cassandra") \
#         .mode("append") \
#         .options(table="campaign_performance", keyspace="futurense") \
#         .save()
#
#
# def execute():
#     kafka_server = [f"{config.server}:9092"]  # Ensure you have the correct port
#     topic = "test_topic"
#
#     consumer = KafkaConsumer(
#         topic,
#         bootstrap_servers=kafka_server,
#         value_deserializer=lambda m: json.loads(m.decode('utf-8')),
#         auto_offset_reset="latest",
#     )
#
#     campaign_id = 0
#     spark = create_spark_session()
#     for message in consumer:
#         print(message)
#         print(message.value)
#         data = message.value
#         data['campaign_id'] = campaign_id
#         campaign_id += 1
#         insert_into_cassandra(data, spark)
#         break  # Consume only one message
#     spark.stop()
#
#
# with DAG(
#         dag_id="first_dag",
#         schedule_interval="@daily",
#         default_args={
#             "owner": "airflow",
#             "retries": 1,
#             "retry_delay": timedelta(minutes=5),
#             "start_date": datetime(2024, 8, 3),
#         },
#         catchup=False) as dag:
#     execute_task = PythonOperator(
#         task_id="execute",
#         python_callable=execute,
#     )
#
#     execute_task
