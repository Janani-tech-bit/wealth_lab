from fastapi.testclient import TestClient
from api.main import app

client = TestClient(app)


def test_positions_endpoint():
    response = client.get("/positions/A100")
    assert response.status_code == 200

    data = response.json()
    assert isinstance(data, list)
    assert len(data) > 0

    instruments = [p["instrument"] for p in data]
    assert "AAPL" in instruments


def test_position_quantity():
    response = client.get("/positions/A100")
    data = response.json()

    aapl = [p for p in data if p["instrument"] == "AAPL"][0]
    assert aapl["quantity"] > 0


def test_portfolio_endpoint():
    response = client.get("/portfolio/A100")
    assert response.status_code == 200

    portfolio = response.json()
    assert "total_value" in portfolio
    assert portfolio["total_value"] > 0
