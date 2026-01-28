import json
from collections import defaultdict

BRONZE_FILE = "data/bronze/transactions.json"
SILVER_FILE = "data/silver/positions.json"


def load_transactions():
    with open(BRONZE_FILE) as f:
        return json.load(f)


def compute_positions(transactions):
    positions = defaultdict(lambda: {
        "quantity": 0,
        "avg_price": 0,
        "last_updated": None
    })

    for t in transactions:
        key = (t["account_id"], t["instrument"])
        pos = positions[key]

        qty_change = t["quantity"] if t["side"] == "BUY" else -t["quantity"]
        new_qty = pos["quantity"] + qty_change

        if new_qty != 0:
            pos["avg_price"] = (
                (pos["quantity"] * pos["avg_price"] +
                 qty_change * t["price"]) / new_qty
            )

        pos["quantity"] = new_qty
        pos["last_updated"] = t["trade_time"]

    return positions


def write_silver(positions):
    output = []

    for (account_id, instrument), data in positions.items():
        output.append({
            "account_id": account_id,
            "instrument": instrument,
            "quantity": data["quantity"],
            "avg_price": round(data["avg_price"], 2),
            "last_updated": data["last_updated"]
        })

    with open(SILVER_FILE, "w") as f:
        json.dump(output, f, indent=2)


if __name__ == "__main__":
    txns = load_transactions()
    positions = compute_positions(txns)
    write_silver(positions)
    print("✅ Silver positions generated")
