# MindClash MVP

A full-stack multiplayer problem-solving game with registration/login, four quiz arenas, rating-aware 1v1 matchmaking, live match updates, first-correct-answer scoring, Elo updates, a leaderboard, and an optional practice bot.

## Stack
- Frontend: React + Vite
- Backend: FastAPI + SQLAlchemy, hosted locally by Uvicorn or by Azure Functions
- Database: SQLite for local development; PostgreSQL for durable Azure deployments
- Matchmaking cache: Redis sorted sets + an atomic Lua pairing script
- Live match updates: authenticated WebSockets locally; HTTP polling on Azure Functions
- Practice opponent: built-in rating-adjusted bot; no external AI API key required

## Run locally

### 1. Start local services

From the project root, set the following in the same shell you will use to start the backend. Generate them once per local installation. For persistent deployments, store them in a secret manager, not in source control:

```bash
export REDIS_PASSWORD="$(python -c 'import secrets; print(secrets.token_hex(32))')"
export JWT_SECRET="$(python -c 'import secrets; print(secrets.token_hex(32))')"
export REDIS_URL="redis://mindclash:${REDIS_PASSWORD}@127.0.0.1:6379/0"
docker compose up -d
```

PowerShell equivalent:

```powershell
$env:REDIS_PASSWORD = python -c "import secrets; print(secrets.token_hex(32))"
$env:JWT_SECRET = python -c "import secrets; print(secrets.token_hex(32))"
$env:REDIS_URL = "redis://mindclash:$($env:REDIS_PASSWORD)@127.0.0.1:6379/0"
docker compose up -d
```

The Compose file starts Redis and Azurite. Both are bound to loopback for local development.

### 2. Run as an Azure Function locally

Install Azure Functions Core Tools v4 and Python 3.12. Copy the local settings example and replace the JWT and Redis placeholders with the values generated above:

```powershell
Copy-Item .\backend\local.settings.example.json .\backend\local.settings.json
```

In another terminal, build the React app for the same-origin Functions host and stage its output:

```powershell
Set-Location .\frontend
$env:VITE_API_URL = ""
$env:VITE_ENABLE_WEBSOCKETS = "false"
npm ci
npm run build
Copy-Item .\dist ..\backend\static -Recurse -Force
Set-Location ..\backend
func start
```

Open the local Functions host at http://localhost:7071. The host uses Azurite from Compose for its local storage account and SQLite for application data. Local websocket testing remains available through the existing Uvicorn/Vite workflow below.

### 3. Run the existing development servers

To develop with live WebSockets, use the existing two-server setup. Start the API and frontend in separate terminals:

```powershell
Set-Location .\backend
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
python -m uvicorn main:app --reload --host 127.0.0.1 --port 8000
```

```powershell
Set-Location .\frontend
npm ci
npm run dev
```

### 4. Deploy

The GitHub Actions workflow builds the frontend and backend dependencies into the Functions package, runs the backend tests, and uploads the deployable package as an artifact. Azure deployment on pushes to `main` (or manual dispatch) is opt-in: set the `AZURE_OIDC_ENABLED` repository variable to `true`, configure the `AZURE_CLIENT_ID`, `AZURE_TENANT_ID`, and `AZURE_SUBSCRIPTION_ID` repository secrets, and grant the identity deployment permission. The Azure tenant must allow GitHub OIDC tokens for the repository. If it does not, download the tested artifact and deploy it using an authenticated Azure CLI session. The Function app must also have `DATABASE_URL`, `REDIS_URL`, and `JWT_SECRET` configured as Key Vault-backed application settings.

The original infrastructure plan doesn't include a durable relational database or an application Redis service. Azure Functions' local SQLite filesystem isn't durable across scale-out or restarts, so a working Azure deployment also requires PostgreSQL Flexible Server and Azure Managed Redis. These are additional billable resources; configure their TLS-enabled connection URLs in Key Vault before deploying.

The app serves the React bundle and API from one Functions hostname, so no custom domain is required. Add a custom domain and certificate only after DNS ownership is available.

Azure Functions serves the API over HTTP. The production frontend therefore uses polling rather than WebSockets.
The `/health` endpoint checks both PostgreSQL and Redis and returns `503` if either service is unavailable.

### 5. Check local Redis

```powershell
docker compose exec redis redis-cli --user mindclash --askpass ping
```

Enter the generated Redis password at the prompt. The backend requires `REDIS_URL` and a randomly generated `JWT_SECRET` of at least 32 bytes. Rotating the JWT key signs existing users out. Rotate before rollout if an instance ever used the old default key or retained account tokens in WebSocket request logs. The client now requests a single-use, match-specific, 30-second ticket over authenticated HTTP; redact query strings at local reverse proxies and use HTTPS in production.

## Test human matchmaking

1. Register Player A in a normal browser window.
2. Register Player B in an incognito/private window (or another browser).
3. Both browser windows must use this same backend and database.
4. Choose the same arena and click **Find opponent** in both windows.
5. The Redis queue pairs the closest eligible ratings (initial range ±100; it widens as players wait).
6. Both clients receive match updates over WebSockets. If a WebSocket disconnects, the frontend falls back to HTTP polling.

You can also click **Play practice bot instead** from the matchmaking screen. Practice wins, losses and forfeits do not change leaderboard stats. The built-in practice opponent uses a rating-adjusted solve probability and runs locally without an external model.

## Matchmaking and concurrency notes

- Redis sorted sets hold per-arena waiting queues.
- A Lua script selects the closest eligible rating and removes both users in one atomic Redis operation, preventing two requests from claiming the same queued player.
- Redis queue entries expire after 10 minutes of inactivity; users can cancel search.
- The match answer endpoint uses a conditional database update on `(match_id, current_question, status)` so only one correct submission can claim and advance a question.
- WebSocket connections are managed in the FastAPI process. For multi-worker/multi-instance production deployments, add Redis Pub/Sub (or a dedicated event broker) for cross-process WebSocket fan-out, and use PostgreSQL row locking/transactions.
- The built-in bot is a heuristic practice opponent, not a generative AI/LLM. It is intentionally simple and should be labelled as a practice bot in the product UI.

## Current MVP limitations

- The question bank contains demo multiple-choice questions, not a curated production bank.
- The DSA arena currently tests concepts rather than executing arbitrary source code. Do not execute user-submitted code inside the API process; add a separate isolated execution service if code challenges are introduced.
- The bot runs as a background task in the API process. In production, move bot jobs to a durable worker/queue.
- Add email verification, password reset, rate limiting, abuse controls, observability, backups, and stronger deployment security before public launch.
