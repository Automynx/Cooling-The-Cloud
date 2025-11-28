# API Documentation

## Cooling The Cloud - REST API

Base URL: `http://localhost:5000` (development)

---

## Endpoints

### Health Check

**GET** `/api/health`

Check API server status and database connection.

**Response:**
```json
{
  "status": "healthy",
  "timestamp": "2024-01-15T10:30:00.000000",
  "supabase_connected": true
}
```

---

### Run Optimization

**POST** `/api/optimize`

Run the data center cooling optimization model.

**Request Body:**
```json
{
  "date": "2024-01-15",
  "use_real_data": true,
  "capacity_mw": 2000
}
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `date` | string | today | Target date (YYYY-MM-DD) |
| `use_real_data` | boolean | true | Use Supabase data or demo data |
| `capacity_mw` | number | 2000 | Data center capacity in MW |

**Response:**
```json
{
  "success": true,
  "results": {
    "summary": {
      "total_cost": 125000.50,
      "electricity_cost": 115000.00,
      "water_cost": 10000.50,
      "peak_demand_mw": 1800
    },
    "savings": {
      "daily_savings": 15000.00,
      "annual_savings": 5475000.00,
      "percentage_saved": 12.6
    },
    "environmental": {
      "water_used_gallons": 50000,
      "water_saved_gallons": 150000,
      "peak_reduction_mw": 200,
      "carbon_avoided_tons": 45.5
    },
    "hourly_data": [
      {
        "hour": 0,
        "temperature": 85,
        "price": 0.05,
        "load_mw": 1500,
        "water_cooling": true,
        "cost": 75.00
      }
    ],
    "metadata": {
      "date": "2024-01-15",
      "source": "supabase",
      "run_id": "abc123",
      "capacity_mw": 2000
    }
  }
}
```

---

### Get Optimization History

**GET** `/api/history`

Retrieve past optimization runs.

**Query Parameters:**
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `limit` | integer | 10 | Number of records to return |

**Example:** `/api/history?limit=20`

**Response:**
```json
{
  "success": true,
  "history": [
    {
      "run_id": "abc123",
      "run_timestamp": "2024-01-15T10:30:00",
      "total_cost": 125000.50,
      "savings_percent": 12.6,
      "capacity_mw": 2000
    }
  ]
}
```

---

### Get Period Summary

**GET** `/api/period-summary`

Get aggregated statistics for a time period.

**Query Parameters:**
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `days` | integer | 30 | Number of days to summarize |

**Example:** `/api/period-summary?days=7`

**Response:**
```json
{
  "success": true,
  "summary": {
    "total_savings": 450000.00,
    "avg_daily_savings": 15000.00,
    "avg_savings_percent": 12.5,
    "total_water_saved_gallons": 4500000
  }
}
```

---

### Get Monthly Breakdown

**GET** `/api/monthly-breakdown`

Get month-by-month cost breakdown.

**Query Parameters:**
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `months` | integer | 6 | Number of months to return |

**Example:** `/api/monthly-breakdown?months=12`

**Response:**
```json
{
  "success": true,
  "breakdown": [
    {
      "month": "2024-01",
      "total_cost": 3750000.00,
      "total_savings": 450000.00,
      "optimization_runs": 31
    }
  ]
}
```

---

### Get Daily Trends

**GET** `/api/daily-trends`

Get daily trend data for charts.

**Query Parameters:**
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `days` | integer | 30 | Number of days of data |

**Example:** `/api/daily-trends?days=14`

**Response:**
```json
{
  "success": true,
  "trends": {
    "dates": ["2024-01-01", "2024-01-02"],
    "costs": [125000, 128000],
    "savings": [15000, 14500],
    "temperatures": [95, 98]
  }
}
```

---

### Get Real-Time Data

**GET** `/api/real-time-data`

Get current temperature and electricity price data.

**Query Parameters:**
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `date` | string | today | Target date (YYYY-MM-DD) |

**Example:** `/api/real-time-data?date=2024-01-15`

**Response:**
```json
{
  "success": true,
  "data": {
    "temperatures": [75, 76, 78, 82, 88, 95, 102, 108, 112, 115, 118, 116, 112, 108, 102, 95, 88, 82, 78, 76, 75, 74, 73, 72],
    "electricity_prices": [0.03, 0.03, 0.03, 0.03, 0.03, 0.03, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.15, 0.15, 0.15, 0.15, 0.15, 0.05, 0.05, 0.03, 0.03],
    "water_prices": [3.24],
    "metadata": {
      "date": "2024-01-15",
      "max_temp": 118,
      "min_temp": 72,
      "avg_price": 0.067,
      "peak_price": 0.15,
      "off_peak_price": 0.03
    }
  }
}
```

---

### Get System Stats

**GET** `/api/stats`

Get overall system statistics.

**Response:**
```json
{
  "success": true,
  "stats": {
    "total_records": 50000,
    "total_optimization_runs": 365,
    "last_30_days_savings": 450000.00,
    "last_year_savings": 5475000.00,
    "avg_daily_savings": 15000.00,
    "avg_savings_percent": 12.6
  }
}
```

---

## Error Responses

All endpoints return errors in this format:

```json
{
  "success": false,
  "error": "Error message description"
}
```

HTTP Status Codes:
- `200` - Success
- `400` - Bad Request (invalid parameters)
- `500` - Server Error

---

## Usage Examples

### cURL

```bash
# Health check
curl http://localhost:5000/api/health

# Run optimization
curl -X POST http://localhost:5000/api/optimize \
  -H "Content-Type: application/json" \
  -d '{"date": "2024-01-15", "capacity_mw": 2000}'

# Get history
curl "http://localhost:5000/api/history?limit=10"

# Get real-time data
curl "http://localhost:5000/api/real-time-data?date=2024-01-15"
```

### JavaScript (fetch)

```javascript
// Run optimization
const response = await fetch('http://localhost:5000/api/optimize', {
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify({
    date: '2024-01-15',
    capacity_mw: 2000,
    use_real_data: true
  })
});
const data = await response.json();
```

### Python (requests)

```python
import requests

# Run optimization
response = requests.post('http://localhost:5000/api/optimize', json={
    'date': '2024-01-15',
    'capacity_mw': 2000,
    'use_real_data': True
})
results = response.json()
```
