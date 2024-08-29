try:
    from datetime import timedelta
    from airflow import DAG
    from airflow.operators.python_operator import PythonOperator
    from datetime import datetime
    from kafka import KafkaConsumer
    import json
    from cassandra.cluster import Cluster
    import config
    from sklearn.impute import KNNImputer
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

def impute_column(df, col):
    df_copy = df.copy()

    if pd.api.types.is_numeric_dtype(df_copy[col]):
        imputer = KNNImputer(n_neighbors=5)
        df_copy[col] = imputer.fit_transform(df_copy[[col]]).flatten()  # Flatten the result
    else:
        le = LabelEncoder()
        nan_mask = df_copy[col].isna()
        non_nan_values = df_copy.loc[~nan_mask, col]

        if len(non_nan_values) > 0:
            le.fit(non_nan_values)
            df_copy.loc[~nan_mask, col] = le.transform(non_nan_values)

        imputer = SimpleImputer(strategy='most_frequent')
        df_copy[col] = imputer.fit_transform(df_copy[[col]]).flatten()  # Flatten the result

        if len(non_nan_values) > 0:
            df_copy[col] = le.inverse_transform(df_copy[col].astype(int))

    return df_copy

def clean(data_org):
    if isinstance(data_org, dict):
        data_org = [data_org]

    data = pd.DataFrame(data_org).copy()
    data = clean_dataframe(data)

    # Rename columns to match the Cassandra table
    data = data.rename(columns={
        'Ad Name': 'ad_name',
        'Campaign Name': 'campaign_name',
        'lead_id.1': 'lead_id_1'
    })

    # Fill NaN values with appropriate defaults
    data['contacted'] = data['contacted'].fillna('Not Contacted')
    data['mapped'] = data['mapped'].fillna("Not Mapped")
    data['ad_name'] = data['ad_name'].fillna("Unknown Ad")
    data['campaign_name'] = data['campaign_name'].fillna("Unknown Campaign")
    data['form_name'] = data['form_name'].fillna("Unknown Form")

    # Apply imputation only if the column exists
    for col in ['contacted', 'lsq_source', 'lsq_lead_stage']:
        if col in data.columns:
            data = impute_column(data, col)

    # Convert lead_id_1 to UUID if it's not already
    if 'lead_id_1' in data.columns:
        def convert_to_uuid(x):
            if x:
                try:
                    return uuid.UUID(x)
                except ValueError:
                    print(f"Invalid UUID: {x}")
                    return None
            return None

        data['lead_id_1'] = data['lead_id_1'].apply(convert_to_uuid)

    # Don't drop NaN values, as we've filled them with defaults
    # data.dropna(inplace=True)

    # If the dataframe is empty after cleaning, return None
    if data.empty:
        return None

    return data.to_dict(orient="records")[0]

def insert_into_campaign_leads(data):
    cluster = Cluster([config.server], port=9042)
    session = cluster.connect('futurense')
    query = """
    INSERT INTO campaign_leads (
        lead_id, created_time, ad_name, campaign_name, contacted, form_name, 
        graduation_degree, graduation_percentage, lead_id_1, lsq_lead_owner, 
        lsq_lead_stage, lsq_source, mapped, platform, work_experience
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    prepared = session.prepare(query)
    lead_id_1 = data.get('lead_id_1')
    if lead_id_1 is not None and not isinstance(lead_id_1, uuid.UUID):
        try:
            lead_id_1 = uuid.UUID(lead_id_1)
        except ValueError:
            print(f"Invalid UUID for lead_id_1: {lead_id_1}")
            lead_id_1 = None

    # Use get() method to provide default values for potentially missing keys
    session.execute(prepared, (
        data.get('lead_id'),
        data.get('created_time'),
        data.get('ad_name', 'Unknown Ad'),
        data.get('campaign_name', 'Unknown Campaign'),
        data.get('contacted', 'Not Contacted'),
        data.get('form_name', 'Unknown Form'),
        data.get('graduation_degree'),
        data.get('graduation_percentage'),
        data.get('lead_id_1'),
        data.get('lsq_lead_owner'),
        data.get('lsq_lead_stage'),
        data.get('lsq_source'),
        data.get('mapped', 'Not Mapped'),
        data.get('platform'),
        data.get('work_experience')
    ))

    session.shutdown()
    cluster.shutdown()

def consume_from_kafka():
    kafka_server = [config.server + ":9092"]
    topic = "leads_generated_topic"

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=kafka_server,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="latest",
    )

    for message in consumer:
        print("Received message:", message.value)
        try:
            data = clean(message.value)
            if data is not None:
                try:
                    insert_into_campaign_leads(data)
                except Exception as e:
                    print(f"Error inserting data into Cassandra: {e}")
            else:
                print("Data was cleaned to empty, skipping insertion")
        except Exception as e:
            print(f"Error processing message: {e}")

with DAG(
    dag_id="leads",
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