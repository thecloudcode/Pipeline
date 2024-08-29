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

    def fix_currency(value):
        if pd.isnull(value):
            return value
        if isinstance(value, str) and '$' in value:
            return float(value.replace('$', '')) * 75
        return value

    def safe_strip(x):
        return x.strip() if isinstance(x, str) else x

    df = df.drop_duplicates()

    for column in df.columns:
        if df[column].dtype == 'object':

            df[column] = df[column].apply(lambda x: re.sub(r'http\S+', '', str(x)) if pd.notnull(x) else x)
            df[column] = df[column].apply(remove_html_tags)
            df[column] = df[column].apply(standardize_capitalization)
            df[column] = df[column].apply(fix_currency)

        elif df[column].dtype in ['int64', 'float64']:
            pass

        if df[column].dtype == 'object':
            df[column] = df[column].apply(convert_date)

    df = df.applymap(safe_strip)

    return df

def impute(df, col):
    df = df.copy()

    le = LabelEncoder()
    df['encoded'] = le.fit_transform(df[col].astype(str))

    imputer = SimpleImputer(strategy='most_frequent')
    df['encoded'] = imputer.fit_transform(df[['encoded']])

    df[col] = le.inverse_transform(df['encoded'])
    df = df.drop('encoded', axis=1)

    return df


def clean(data_org):
    if isinstance(data_org, dict):
        data_org = [data_org]

    data = pd.DataFrame(data_org).copy()
    data = clean_dataframe(data)

    data.drop(columns=['campaign_start_date'], inplace=True)
    data = impute(data, 'adset_name')

    data.dropna(inplace=True)
    data = data[data['adset_name'] != 'nan']

    if 'campaign_name' in data_org[0]:
        data['campaign_name'] = data_org[0]['campaign_name']
    if 'creative_name' in data_org[0]:
        data['creative_name'] = data_org[0]['creative_name']

    return data.to_dict(orient="records")[0]



def insert_into_campaingperformance(data):
    cluster = Cluster([config.server], port=9042)
    session = cluster.connect('futurense')
    query = """
    INSERT INTO campaignperformance (
        dates, campaign_name, creative_name, adset_name, platform, 
        total_spent, impressions, clicks, click_through_rate, leads
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    prepared = session.prepare(query)

    session.execute(prepared, (
        data['dates'],
        data['campaign_name'],
        data['creative_name'],
        data['adset_name'],
        data['platform'],
        data['total_spent'],
        data['impressions'],
        data['clicks'],
        data['click_through_rate'],
        data['leads']
    ))

    session.shutdown()
    cluster.shutdown()

def consume_from_kafka():
    kafka_server = [config.server + ":9092"]
    topic = "campaign_performance_topic"

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
            insert_into_campaingperformance(data)
        except Exception as e:
            print(f"Error inserting data into Cassandra: {e}")

with DAG(
    dag_id="campaigns",
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
