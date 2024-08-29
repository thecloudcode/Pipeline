import pandas as pd
import re
from sklearn.preprocessing import LabelEncoder
from sklearn.impute import SimpleImputer

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

def execute(data_org):

    data_org = pd.DataFrame(data_org)
    data = data_org.copy()
    data = clean_dataframe(data)

    data.drop(columns=['campaign_start_date'], inplace=True)
    data = impute(data, 'adset_name')

    data.dropna(inplace=True)
    data = data[data['adset_name'] != 'nan']

    data['campaign_name'] = data_org['campaign_name']
    data['creative_name'] = data_org['creative_name']

    return data.to_dict(orient="records")[0]

