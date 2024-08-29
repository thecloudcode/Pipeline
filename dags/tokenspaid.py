try:
    from datetime import timedelta
    from airflow import DAG
    from airflow.operators.python_operator import PythonOperator
    from datetime import datetime
    from kafka import KafkaConsumer
    import json
    from cassandra.cluster import Cluster
    import config
    import time
    import pandas as pd
    import re
    from sklearn.preprocessing import LabelEncoder
    from sklearn.impute import SimpleImputer
    import uuid

except Exception as e:
    print("Error  {} ".format(e))

def clean_dataframe(df):
    def remove_html_tags(text):
        return re.sub('<.*?>', '', str(text)) if pd.notnull(text) else text

    def standardize_capitalization(text):
        return str(text).lower() if pd.notnull(text) else text

    def convert_date(value):
        if pd.isnull(value):
            return value
        try:
            return pd.to_datetime(value)
        except:
            return value

    def safe_strip(x):
        return x.strip() if isinstance(x, str) else x

    df = df.drop_duplicates()

    for column in df.columns:
        if df[column].dtype == 'object':

            df[column] = df[column].apply(lambda x: re.sub(r'http\S+', '', str(x)) if pd.notnull(x) else x)
            df[column] = df[column].apply(remove_html_tags)
            df[column] = df[column].apply(standardize_capitalization)

        elif df[column].dtype in ['int64', 'float64']:
            pass

        if df[column].dtype == 'object':
            df[column] = df[column].apply(convert_date)

    df = df.applymap(safe_strip)

    return df


def clean(data_org):
    if isinstance(data_org, dict):
        data_org = [data_org]

    data = pd.DataFrame(data_org).copy()

    data = clean_dataframe(data)

    # Check for required fields
    required_fields = ['prospect_id', 'created_date', 'name', 'paid_date', 'source', 'status']
    for field in required_fields:
        if field not in data.columns or data[field].isnull().all():
            print(f"Required field '{field}' is missing or all null")
            return None

    # Fill NaN values with appropriate defaults
    data['source'] = data['source'].fillna("No Source")
    data['counsellor'] = data['counsellor'].fillna("No Counsellor")
    data['status'] = data['status'].fillna("INACTIVE TOKEN")
    data['agent'] = data['agent'].fillna("No Agent")
    data['cohort'] = data['cohort'].fillna("No Cohort")
    data['lead_id'] = data['lead_id'].fillna("No Lead ID")

    # Convert dates to proper format
    data['created_date'] = pd.to_datetime(data['created_date']).dt.date
    data['paid_date'] = pd.to_datetime(data['paid_date']).dt.date

    # Convert prospect_id to UUID
    data['prospect_id'] = data['prospect_id'].apply(lambda x: uuid.UUID(x) if x else None)

    if data.empty:
        return None

    return data.to_dict(orient="records")[0]

def insert_into_tokens_paid(data):
    cluster = Cluster([config.server], port=9042)
    session = cluster.connect('futurense')
    query = """
    INSERT INTO tokens_paid (
        prospect_id, created_date, agent, cohort, counsellor, lead_id, name,
        paid_date, source, status, upload_timestamp
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    prepared = session.prepare(query)

    try:
        session.execute(prepared, (
            data['prospect_id'],
            data['created_date'],
            data['agent'],
            data['cohort'],
            data['counsellor'],
            data['lead_id'],
            data['name'],
            data['paid_date'],
            data['source'],
            data['status'],
            datetime.now()  # Use current timestamp for upload_timestamp
        ))
        print("Data inserted successfully")
    except Exception as e:
        print(f"Error executing Cassandra query: {e}")
    finally:
        session.shutdown()
        cluster.shutdown()


def consume_from_kafka():
    kafka_server = [config.server + ":9092"]
    topic = "tokens_paid_topic"

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=kafka_server,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="latest",
    )

    for message in consumer:
        print("Received message:", message.value)
        data = clean(message.value)
        if data is not None:
            try:
                insert_into_tokens_paid(data)
            except Exception as e:
                print(f"Error inserting data into Cassandra: {e}")
        else:
            print("Cleaned data is None, skipping insertion")

with DAG(
    dag_id="tokenspaid",
    schedule_interval="*/1 * * * *",
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=7),
        "start_date": datetime(2024, 8, 3),
    },
    catchup=False
) as dag:

    execute_task = PythonOperator(
        task_id="consume_from_kafka",
        python_callable=consume_from_kafka,
    )
    execute_task