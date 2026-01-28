from fastapi import FastAPI
from datetime import datetime, timedelta, timezone

import json


app = FastAPI()


@app.get("/health")
def health():
    return {"status": "ok"}

HOT_FILE = "data/gold/hot_positions.json"
COLD_FILE = "data/gold/cold_positions.json"


def load(path):
    with open(path) as f:
        return json.load(f)


@app.get("/positions/{account_id}")
def get_positions(account_id: str):
    cutoff = datetime.now(timezone.utc) - timedelta(days=3)

    hot = load(HOT_FILE)
    cold = load(COLD_FILE)

    result = []

    for p in hot + cold:
        if p["account_id"] == account_id:
            result.append(p)

    return result

@app.get("/portfolio/{account_id}")
def get_portfolio(account_id: str):
    positions = get_positions(account_id)

    total_value = sum(
        p["quantity"] * p["avg_price"] for p in positions
    )

    return {
        "account_id": account_id,
        "total_value": round(total_value, 2),
        "positions": positions
    }
