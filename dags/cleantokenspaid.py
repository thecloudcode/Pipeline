from cleanphonemetrics import clean_dataframe

def execute(data_org):
    data = data_org.copy()

    data = clean_dataframe(data)
    data['Lead ID'] = data_org['Lead ID']
    data.drop(columns=['upload_timestamp'], inplace=True)
    data['source'].fillna("No Source", inplace=True)
    data['counsellor'].fillna("No Counsellor", inplace=True)
    data['status'].fillna("INACTIVE TOKEN", inplace=True)

    data2 = data.copy()
    data2.drop(columns=['Lead ID'], inplace=True)
    data2.dropna(inplace=True)
    data.dropna(inplace=True)

    return data, data2