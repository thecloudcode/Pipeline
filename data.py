import pandas as pd
from fastapi import FastAPI
from itertools import cycle
import uvicorn

app = FastAPI()

campaignperformance = pd.read_csv("../../Data Pipeline/Raw Data/Extracted/campaign_performance.csv")
leadsgenerated = pd.read_csv("../../Data Pipeline/Raw Data/Extracted/Leads Generated.csv")
phonemetrics = pd.read_csv("../../Data Pipeline/Raw Data/Extracted/Phone Metrics.csv")
tokenspaid = pd.read_csv("../../Data Pipeline/Raw Data/Extracted/Tokens Paid.csv")
candidateapplicationtracker = pd.read_csv("../../Data Pipeline/Raw Data/Extracted/Candidate Application Tracker.csv")
webinarleads = pd.read_csv("../../Data Pipeline/Raw Data/Extracted/Webinar Leads.csv")

data_cycle1 = cycle(campaignperformance.to_dict(orient='records'))
data_cycle2 = cycle(leadsgenerated.to_dict(orient='records'))
data_cycle3 = cycle(phonemetrics.to_dict(orient='records'))
data_cycle4 = cycle(tokenspaid.to_dict(orient='records'))
data_cycle5 = cycle(candidateapplicationtracker.to_dict(orient='records'))
data_cycle6 = cycle(webinarleads.to_dict(orient='records'))

def sanitize_data(data):
    for key, value in data.items():
        if isinstance(value, float) and (value != value or value in [float('inf'), float('-inf')]):
            data[key] = None
    return data

@app.get("/campaignperformance")
async def get_campaign():
    try:
        data = next(data_cycle1)
        sanitized_data = sanitize_data(data)
        return sanitized_data
    except StopIteration:
        pass

@app.get("/leadsgenerated")
async def get_leads():
    try:
        data = next(data_cycle2)
        sanitized_data = sanitize_data(data)
        return sanitized_data
    except StopIteration:
        pass

@app.get("/phonemetrics")
async def get_phonemetrics():
    try:
        data = next(data_cycle3)
        sanitized_data = sanitize_data(data)
        return sanitized_data
    except StopIteration:
        pass

@app.get("/tokenspaid")
async def get_tokens():
    try:
        data = next(data_cycle4)
        sanitized_data = sanitize_data(data)
        return sanitized_data
    except StopIteration:
        pass

@app.get("/candidateapplicationtracker")
async def get_cat():
    try:
        data = next(data_cycle5)
        sanitized_data = sanitize_data(data)
        return sanitized_data
    except StopIteration:
        pass

@app.get("/webinars")
async def get_webinars():
    try:
        data = next(data_cycle6)
        sanitized_data = sanitize_data(data)
        return sanitized_data
    except StopIteration:
        pass

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)