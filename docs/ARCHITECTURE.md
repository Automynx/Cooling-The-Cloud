# System Architecture

## Overview

Cooling The Cloud is a full-stack optimization system for Arizona data center operations. The system ingests real-time electricity and weather data, runs a linear optimization model, and presents results through a React dashboard.

---

## System Diagram

```mermaid
flowchart TB
    subgraph External["External Data Sources"]
        EIA[("EIA API\n(Electricity Prices)")]
        NOAA[("NOAA API\n(Weather Data)")]
    end

    subgraph Database["Database Layer"]
        Supabase[(Supabase\nPostgreSQL)]
    end

    subgraph Backend["Python Backend"]
        DataInterface["Data Interface\n(data_interface.py)"]
        Optimizer["Pyomo Optimizer\n(optimizer_linear.py)"]
        API["Flask REST API\n(api_server.py)"]
    end

    subgraph Frontend["React Frontend"]
        Dashboard["Dashboard\n(Real-time Charts)"]
        History["History View\n(Past Optimizations)"]
        RealTime["Real-Time Monitor\n(Live Data)"]
    end

    EIA -->|Hourly Prices| Supabase
    NOAA -->|Temperature Data| Supabase
    Supabase --> DataInterface
    DataInterface --> Optimizer
    Optimizer -->|Results| API
    Supabase -->|Historical Data| API
    API -->|JSON| Dashboard
    API -->|JSON| History
    API -->|JSON| RealTime
```

---

## Data Flow

```mermaid
sequenceDiagram
    participant User
    participant React as React Frontend
    participant Flask as Flask API
    participant Optimizer as Pyomo Optimizer
    participant DB as Supabase

    User->>React: Click "Run Optimization"
    React->>Flask: POST /api/optimize
    Flask->>DB: Fetch temperature & price data
    DB-->>Flask: Return 24-hour data
    Flask->>Optimizer: Build & solve model
    Optimizer-->>Flask: Optimization results
    Flask->>DB: Store results
    Flask-->>React: JSON response
    React-->>User: Display charts & savings
```

---

## Component Details

### Data Sources

| Source | Data | Frequency | Format |
|--------|------|-----------|--------|
| EIA API | Electricity prices, grid demand | Hourly | JSON |
| NOAA API | Temperature, humidity | Hourly | JSON |
| Supabase | Historical data, optimization results | Real-time | PostgreSQL |

### Optimization Engine

```mermaid
flowchart LR
    subgraph Inputs
        T["Temperature\n(24 hours)"]
        P["Electricity Prices\n(24 hours)"]
        C["Constraints\n(Capacity, Peak)"]
    end

    subgraph Model["Linear Optimization Model"]
        Obj["Minimize:\nElectricity Cost +\nWater Cost"]
        Dec["Decision Variables:\n- Hourly load (MW)\n- Cooling mode\n- Flexible load shift"]
    end

    subgraph Outputs
        Schedule["Optimal Schedule"]
        Savings["Cost Savings"]
        Water["Water Conservation"]
    end

    T --> Model
    P --> Model
    C --> Model
    Model --> Schedule
    Model --> Savings
    Model --> Water
```

### Tech Stack

```
Frontend:        React 18 + Vite + TailwindCSS + Recharts
API:             Flask + Flask-CORS
Optimization:    Pyomo + HiGHS Solver
Database:        Supabase (PostgreSQL)
Data Processing: Pandas + NumPy
```

---

## Directory Structure

```
Cooling-The-Cloud/
├── cooling-cloud-react/     # React frontend
│   ├── src/
│   │   ├── pages/           # Dashboard, History, RealTime
│   │   ├── components/      # UI components
│   │   └── services/        # API client
│   └── package.json
├── model/                   # Optimization engine
│   ├── optimizer_linear.py  # Pyomo linear model
│   └── data_interface.py    # Data loading/validation
├── api_server.py            # Flask REST API
├── data/                    # Database interfaces
│   └── supabase_interface.py
├── scripts/                 # Data fetching scripts
│   ├── fetch_eia.py
│   └── fetch_prices.py
├── tests/                   # Test files
└── docs/                    # Documentation
```

---

## API Architecture

```mermaid
flowchart LR
    subgraph Endpoints
        H["/api/health"]
        O["/api/optimize"]
        Hi["/api/history"]
        S["/api/stats"]
        RT["/api/real-time-data"]
    end

    subgraph Methods
        GET["GET"]
        POST["POST"]
    end

    GET --> H
    POST --> O
    GET --> Hi
    GET --> S
    GET --> RT
```

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/api/health` | GET | Health check |
| `/api/optimize` | POST | Run optimization |
| `/api/history` | GET | Past results |
| `/api/stats` | GET | System statistics |
| `/api/real-time-data` | GET | Current conditions |
| `/api/period-summary` | GET | Period aggregates |
| `/api/monthly-breakdown` | GET | Monthly costs |
| `/api/daily-trends` | GET | Trend data |

---

## Deployment Options

```mermaid
flowchart TB
    subgraph Local["Local Development"]
        L1["npm run dev\n(port 3000)"]
        L2["python api_server.py\n(port 5000)"]
    end

    subgraph Production["Production (Vercel)"]
        P1["React Static Build"]
        P2["Serverless Functions\n(api/index.py)"]
        P3["Supabase Cloud"]
    end

    Local -->|Deploy| Production
```
