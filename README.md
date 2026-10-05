# Deriv 10-Minute Window Auto-Trader and Collector

A high-performance algorithmic trading bot and dashboard for Deriv Synthetic Indices (Volatility 100, 75, 50, etc.). Built with FastAPI, WebSockets, HTML5 Canvas, and modern cyber-dark aesthetics inspired by TonWeb3/64.

## How it Works

The bot operates on a **unified continuous learning & memory-managed architecture**:
- **Window 1 (Initial 10-Minute Baseline):** Gathers live ticks exclusively to establish statistical boundaries (rolling volatility $\sigma$, mean absolute change $\mu$, streak runs, velocity). **Zero trades are opened** during this period.
- **Continuous 10-Minute Learning & Trading:** At the end of every 10-minute window, the engine backtests 64 candidate rules, updates its persistent **Cumulative KnowledgeBase** (tracking lifetime trade volume, empirical win rate, and Bayesian blended confidence for each rule), and trades the highest-ranking statistical rule while continuing to gather new knowledge.
- **1-Hour Periodic Memory Maintenance:** Every 1 hour, the system archives and flushes all CSV records, and discards raw in-memory tick queues older than 1 hour to prevent server RAM overload and lag—**without forgetting what it has learned** (the cumulative knowledge base and model parameters remain intact).
- **Window-Isolated Ticks Display:** The dashboard chart and ticks table strictly display ticks for the active 10-minute window, resetting on each window rollover for clean, uncluttered monitoring.
- **Auditable CSV Outputs:** Saves `ticks_*.csv`, `trades_*.csv`, and `cycles_*.csv` in the `data/` directory.

## Settings (`/settings`)

Configure your Deriv connection, risk management, and trading parameters:
- **Deriv Credentials:** Token (saved locally on the server, never exposed in client scripts), App ID, and Account ID.
- **Risk Management:** Fixed stake or % of balance, maximum stake cap, session loss limit, profit target, and consecutive loss circuit-breaker.
- **Execution Safeguards:** Demo / Live account toggle with explicit safety confirmation for real money accounts.
- **Engine Configuration:** 10-minute window duration, strictness filter, minimum trade counts, and edge margin.

## Quickstart

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Start the Server
```bash
python main.py
```
Open [http://127.0.0.1:8000](http://127.0.0.1:8000) in your browser.

## Deployment (Railway / Docker)

1. Create a service from this repository (uses Dockerfile).
2. Attach a **Railway Volume** mounted at `/data` to persist settings and session CSVs.
3. Configure environment variables if desired (can also be configured via `/settings` in the UI).
4. Run as a single replica (session state is managed in-memory).

## Environment Variables

| Variable | Default | Purpose |
|---|---|---|
| `DERIV_TOKEN` | none | Fallback API token; overridden by the Settings page. |
| `DERIV_APP_ID` | none | Deriv App ID for API requests. |
| `DERIV_ACCOUNT_ID` | auto | Target account ID (Demo or Real). |
| `ALLOW_REAL_TRADING` | `0` | Set to `1` to enable Live real money trading mode. |
| `MAX_STAKE` | none | Server-enforced hard ceiling on trade stake. |
| `MIN_WINDOW_SECONDS` | `60` | Minimum allowed window duration. |
| `NULL_SHUFFLES` | `30` | Number of random shuffles for the luck benchmark filter. |
| `DATA_DIR` | `./data` | Directory where CSV logs and settings are stored. |
| `DASHBOARD_USER` / `DASHBOARD_PASSWORD` | none | Optional HTTP Basic authentication for the dashboard. |

## Strategy Research Backtester

Run an out-of-sample walk-forward test of the 64-rule engine on any tick CSV:
```bash
python -m bot.engine data/ticks_R_100_example.csv
```
