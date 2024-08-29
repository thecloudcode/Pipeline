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

    # Ensure 'Lead Id' is preserved
    if 'Lead Id' in data.columns:
        lead_id = data['Lead Id']
    else:
        lead_id = None

    data.drop(columns=['mobile_number', 'contacted', 'new_followup'], errors='ignore', inplace=True)
    data['inbound_phone_call_counter'].fillna(0, inplace=True)
    data['outbound_phone_call_counter'].fillna(0, inplace=True)
    data['sales_squad'].fillna("No Squad", inplace=True)
    data['squad_role'].fillna("No Role", inplace=True)

    # Reinsert 'Lead Id' if it was present
    if lead_id is not None:
        data['Lead Id'] = lead_id

    data2 = data.dropna(subset=['Lead Id'])
    data.drop(columns=['Lead Id'], errors='ignore', inplace=True)
    data.dropna(inplace=True)
    data2.dropna(inplace=True)

    return data.to_dict(orient="records")[0]

def insert_into_lead_call_activity(data):
    cluster = Cluster([config.server], port=9042)
    session = cluster.connect('futurense')
    query = """
    INSERT INTO lead_call_activity (
        lead_number, date_of_call, activity, call_duration_sec, call_time, calls_done_by,
        calls_done_by2, calls_done_by_without_spaces, first_name,
        inbound_phone_call_counter, lead_id, lead_score, lead_stage,
        outbound_phone_call_counter, owner, sales_squad, squad_role, status
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    prepared = session.prepare(query)

    session.execute(prepared, (
        data['lead_number'],
        data['date_of_call'],
        data['activity'],
        data['call_duration_sec'],
        data['call_time'],
        data['calls_done_by'],
        data['calls_done_by2'],
        data['calls_done_by_without_spaces'],
        data['first_name'],
        data['inbound_phone_call_counter'],
        data.get('Lead Id', None),
        data['lead_score'],
        data['lead_stage'],
        data['outbound_phone_call_counter'],
        data['owner'],
        data['sales_squad'],
        data['squad_role'],
        data['status']
    ))

    session.shutdown()
    cluster.shutdown()

def consume_from_kafka():
    kafka_server = [config.server + ":9092"]
    topic = "phone_metrics_topic"

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=kafka_server,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="latest",
    )

    for message in consumer:
        print("Received message:", message.value)
        data = clean(message.value)
        try:
            insert_into_lead_call_activity(data)
        except Exception as e:
            print(f"Error inserting data into Cassandra: {e}")

with DAG(
    dag_id="phonemetrics",
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
