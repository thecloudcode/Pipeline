import pandas as pd
import re
from cleancampaignperformance import clean_dataframe

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

def execute(data_org):
    data = data_org.copy()

    data = clean_dataframe(data)
    data['Lead Id'] = data_org['Lead Id']
    data.drop(columns=['mobile_number','contacted','new_followup'], inplace=True)
    data['inbound_phone_call_counter'].fillna(0, inplace=True)
    data['outbound_phone_call_counter'].fillna(0, inplace=True)
    data['sales_squad'].fillna("No Squad", inplace=True)
    data['squad_role'].fillna("No Role", inplace=True)

    data2 = data.dropna(subset=['Lead Id'])
    data.drop(columns=['Lead Id'], inplace=True)
    data.dropna(inplace=True)
    data2.dropna(inplace=True)

    return data, data2