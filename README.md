# Deriv Tick Collector

A small dashboard that records live tick data from Deriv's public WebSocket into CSV files.

- Start / Stop button (it flips to Stop while recording)
- Symbols: R_100, 1HZ100V (R_100 1s), R_75, 1HZ75V, R_50, 1HZ50V
- Live price chart, latest-ticks table and a real-time console
- One CSV per recording, downloadable from the dashboard
- Auto-reconnect with backoff, keepalive ping, stale-connection detection

## CSV columns

`received_at_utc, tick_time_utc, epoch, symbol, quote, bid, ask, pip_size, change, tick_id`

`change` is the quote minus the previous tick's quote (empty on the first row).

## Deploy on Railway

1. Push this folder to a GitHub repo.
2. Railway -> New Project -> Deploy from GitHub repo. `railway.json` tells Railway to build with the included `Dockerfile`; Railway sets `PORT` for you.
3. **Add a Volume** (service -> Settings -> Volumes) mounted at `/data`, then set the variable `DATA_DIR=/data`.
   Without a volume, CSV files are lost on every redeploy or restart.
4. Set `DASHBOARD_PASSWORD` (and optionally `DASHBOARD_USER`, default `admin`). Without it the dashboard is open to anyone with the URL.
5. Service -> Settings -> Networking -> Generate Domain, then open the URL.

Keep the service at **one replica**: the recorder state lives in memory.

## Environment variables

| Variable | Default | Purpose |
|---|---|---|
| `DERIV_WS_URL` | `wss://api.derivws.com/trading/v1/options/ws/public` | WebSocket to connect to |
| `DATA_DIR` | `./data` | Where CSVs are written (use a Railway volume, e.g. `/data`) |
| `DASHBOARD_PASSWORD` | empty (no auth) | Enables HTTP basic auth |
| `DASHBOARD_USER` | `admin` | Basic auth user |
| `PING_INTERVAL` | `30` | Seconds between Deriv `ping` messages |
| `STALE_AFTER` | `20` | Reconnect if no message arrives for this many seconds |
| `MAX_BACKOFF` | `30` | Maximum reconnect delay (seconds) |

## Run with Docker

```bash
docker build -t deriv-tick-collector .
docker run --rm -p 8000:8000 -v "$(pwd)/data:/data" \
  -e DASHBOARD_PASSWORD=changeme \
  deriv-tick-collector
```

Open http://localhost:8000. CSVs land in `./data` on your machine. The image writes to `/data` inside the container
(`DATA_DIR=/data`), so always mount a volume there if you want to keep recordings.

The container runs as root on purpose: Railway volumes are mounted root-owned, and a non-root user would not be able to write to them.

## Run locally (no Docker)

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
uvicorn main:app --reload --port 8000
```

Open http://localhost:8000.

### Test without Deriv (mock server)

```bash
python tests/mock_deriv.py &                      # fake ticks on ws://127.0.0.1:9999
DERIV_WS_URL=ws://127.0.0.1:9999 uvicorn main:app --port 8000
```

## Notes

- Ticks are public market data, so no token or App ID is needed.
- If Deriv changes the public endpoint, set `DERIV_WS_URL` instead of editing code.
- Symbols are defined in the `SYMBOLS` dict at the top of `main.py`; add more there (e.g. `R_10`, `1HZ25V`).
- Switching symbol: stop, pick a new symbol, start. Each run writes its own file.
