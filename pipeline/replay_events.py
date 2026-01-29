import json
from datetime import datetime

ICEBERG_FILE = "store/iceberg_history.jsonl"
DYNAMO_FILE = "store/dynamo_hot.json"

def replay():
    print("🔁 Starting event replay")

    # simple demo replay (no filters yet)
    state = {}

    with open(ICEBERG_FILE) as f:
        for line in f:
            event = json.loads(line)
            state[event["account_id"]] = {
                "position": event["position"],
                "updated_at": event["updated_at"]
            }

    with open(DYNAMO_FILE, "w") as f:
        json.dump(state, f, indent=2)

    print("✅ Replay completed")

if __name__ == "__main__":
    replay()
