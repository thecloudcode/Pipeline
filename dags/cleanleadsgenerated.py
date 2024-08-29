import pandas as pd
from sklearn.impute import KNNImputer
from sklearn.preprocessing import LabelEncoder
from sklearn.impute import SimpleImputer
from cleancampaignperformance import clean_dataframe

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


def execute(data_org):
    data = data_org.copy()

    data = clean_dataframe(data)
    data['contacted'] = data['contacted'].fillna('notcontacted')
    data['mapped'] = data['mapped'].fillna("Not Mapped")
    data = impute_column(data, 'contacted')
    data = impute_column(data, 'lsq_source')
    data = impute_column(data, 'lsq_lead_stage')
    data['lead_id'] = data_org['lead_id']
    data.dropna(inplace=True)

    return data