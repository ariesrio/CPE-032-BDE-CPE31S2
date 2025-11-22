# Real-Time Cryptocurrency Streaming Dashboard

## Overview
A streaming system that collects live cryptocurrency prices, stores them in MongoDB, and displays them in an interactive dashboard.

**Tech Stack:** Python + Kafka + MongoDB + Streamlit

---

## Quick Start

### 1. Install
```bash
pip install -r requirements.txt
```

### 2. Start Kafka
```bash
bin/kafka-storage.sh format -t <ID> -c config/server.properties --standalone
bin/kafka-server-start.sh config/server.properties
```

### 3. Start Producer
```bash
python producer.py \
  --mongo-uri "mongodb+srv://..." \
  --symbols bitcoin ethereum binancecoin tether usd-coin \
  --interval 60
```

### 4. Start Dashboard
```bash
streamlit run app.py
```
Open: `http://localhost:8501`

---

## Project Structure

| File | Purpose |
|------|---------|
| `producer.py` | Fetches crypto prices from CoinGecko API, sends to Kafka + MongoDB |
| `app.py` | Streamlit dashboard (real-time + historical charts) |
| `requirements.txt` | Python dependencies |

---

## Components

**Producer (producer.py)**
- Collects prices every 60 seconds (configurable)
- Tracks: Bitcoin, Ethereum, Binance Coin, Tether, USD Coin
- Stores in MongoDB + sends to Kafka

**Dashboard (app.py)**
- Tab 1: Real-time price monitoring with live charts
- Tab 2: Historical analysis with time-range filters
- Sky-blue color theme

---

## Dashboard Features

**Real-Time Tab**
- Current prices for all 5 cryptos
- 24-hour change percentage
- Live trading volume charts

**Historical Tab**
- Select cryptocurrency from dropdown
- Choose time range (1-168 hours)
- View price trends over time
- Download charts as PNG

---

## Data Schema

```json
{
  "_id": "BTC_2025-11-21T10:00:00Z",
  "timestamp": "2025-11-21T10:00:00Z",
  "value": 81450.50,
  "sensor_id": "BTC",
  "bid_price": 81445.50,
  "ask_price": 81455.50,
  "inserted_at": { "$date": "2025-11-21T10:00:00" }
}
```

---

## Query Filters

The dashboard lets you:
- **Select cryptocurrency** (BTC, ETH, BNB, USDT, USDC)
- **Choose time range** (1 to 168 hours)
- **View different metrics** (price, volume, trends)

No data aggregation—just raw data filtered by time window.

---

## Prerequisites

- Python 3.10+
- Apache Kafka (localhost:9092)
- MongoDB Atlas (with connection URI)
- CoinGecko API (free, no key needed)

---

## Troubleshooting

| Error | Solution |
|-------|----------|
| Kafka not connecting | Start Kafka server first |
| No data in dashboard | Run producer for 1-2 minutes first |
| MongoDB error | Check URI and network access |
| API rate limit | Increase `--interval` to 120+ seconds |

---

## Performance

- **Data collected:** ~1,440 points per day (1 per minute × 5 cryptos)
- **Storage:** ~1GB per month per crypto
- **Query response:** < 2 seconds

---

## Future Ideas

- Add Spark for batch processing
- Add price alerts
- Machine learning predictions
- More cryptocurrencies
- Advanced technical analysis

---

**Course:** CPE-032 Big Data Engineering  
**Last Updated:** November 22, 2025
