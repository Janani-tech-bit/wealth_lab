import json
from datetime import datetime, timedelta

ICEBERG_FILE = "store/iceberg_history.jsonl"
DYNAMO_FILE = "store/dynamo_hot.json"

HOT_WINDOW_DAYS = 3

def load_iceberg():
    records = []
    with open(ICEBERG_FILE) as f:
        for line in f:
            records.append(json.loads(line))
    return records

def rebuild_dynamo():
    now = datetime.fromisoformat("2024-01-04T00:00:00")
    cutoff = now - timedelta(days=HOT_WINDOW_DAYS)

    latest_state = {}

    records = load_iceberg()

    # process in event-time order
    records.sort(key=lambda r: r["updated_at"])

    for r in records:
        ts = datetime.fromisoformat(r["updated_at"])
        if ts >= cutoff:
            latest_state[r["account_id"]] = {
                "position": r["position"],
                "updated_at": r["updated_at"]
            }

    with open(DYNAMO_FILE, "w") as f:
        json.dump(latest_state, f, indent=2)

    print("✅ DynamoDB rebuilt from Iceberg")
    print(json.dumps(latest_state, indent=2))


if __name__ == "__main__":
    rebuild_dynamo()
