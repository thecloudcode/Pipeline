import pandas as pd
from fastapi import FastAPI
from itertools import cycle
import uvicorn

app = FastAPI()
data = pd.read_csv("../.././cleaned/CPSortedByDates.csv")
data_cycle = cycle(data.to_dict(orient='records'))

@app.get("/data")
async def get_data():
    return next(data_cycle)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)