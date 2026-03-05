# Yielder Backend

Backend API for Yielder, a multi-chain portfolio and DeFi platform focused on Stellar plus cross-chain routing and analytics.

## What This Service Covers

- Authentication (email, Google, wallet challenge/login/link)
- Wallet management and activity ingestion
- Portfolio valuation, analytics, and performance endpoints
- DeFi flows (yield discovery, optimizer, borrow, bridge/swap tx building)
- RPC health and chain call/broadcast endpoints
- Anchor integration endpoints
- Notifications and admin audit/feature controls
- Billing checkout/portal endpoints

## Tech Stack

- NestJS 10 + Fastify
- MongoDB + Mongoose
- JWT auth
- Swagger/OpenAPI at runtime
- Pino logger

## Repository Layout

```text
src/
  modules/
    auth/
    wallet/
    portfolio/
    defi/
    rpc/
    market/
    oracle/
    realtime/
    notifications/
    billing/
    ...
  main.ts
  modules/app.module.ts
docs/
  API_ROUTES.md
  ARCHITECTURE_OVERVIEW.md
```

## Prerequisites

- Node.js 20+ recommended
- npm
- MongoDB running locally or remotely

## Quick Start

1. Install dependencies.

```bash
npm install
```

2. Copy env template.

```bash
cp .env.example .env
```

3. Set minimum required env values in `.env`:

- `MONGO_URI`
- `JWT_SECRET`
- `JWT_REFRESH_SECRET`
- `PORT` (default used in this project is `8085`)
- `STELLAR_NETWORK` and Horizon URL values

4. Start backend.

```bash
npm run start:dev
```

5. Open API docs.

- `http://127.0.0.1:8085/api/docs`

## Runtime Endpoints

- API base (local): `http://127.0.0.1:8085`
- Swagger: `GET /api/docs`
- Health examples:
  - `GET /realtime/health`
  - `GET /api/stellar/network-stats`

## Scripts

| Script | Purpose |
|---|---|
| `npm run start` | Start app |
| `npm run start:dev` | Watch mode for development |
| `npm run build` | Clean + compile TypeScript |
| `npm run start:prod` | Run compiled app from `dist` |
| `npm run smoke:wallet:evm` | Wallet auth smoke check |
| `npm run smoke:bridge:evm` | Bridge quote/build smoke check |
| `npm run prisma:generate` | Prisma client generation |
| `npm run prisma:migrate` | Prisma migration (if used in your setup) |

## Environment Notes

Full variable template is in `.env.example`.

Important groups:

- Auth/security: `JWT_SECRET`, `JWT_REFRESH_SECRET`, `GOOGLE_CLIENT_ID`, `TURNSTILE_SECRET_KEY`
- Chain/rpc: `STELLAR_NETWORK`, `STELLAR_HORIZON_URL*`, `AXELAR_RPC_URL*`, `RPC_*`
- Market/oracle tuning: `PYTH_*`, `MARKET_*`
- DeFi/bridge: `STELLAR_CIRCLE_CCTP_V2_*`, `BRIDGE_*`, `OPTIMIZER_*`
- Billing URLs: `BILLING_*`

## Reviewer Flow (Suggested)

1. Start backend and open Swagger (`/api/docs`).
2. Get token via `POST /auth/login` or wallet auth endpoints.
3. Verify auth via `GET /auth/me`.
4. Check core data routes:
   - `GET /wallet`
   - `GET /portfolio/summary`
   - `GET /portfolio/analytics`
5. Check infra routes:
   - `GET /rpc/status/:chain`
   - `GET /realtime/health`
6. Check DeFi routes:
   - `GET /defi/yields`
   - `GET /defi/bridge/quote`

## Detailed Documentation

- Route catalog: `docs/API_ROUTES.md`
- Architecture and module map: `docs/ARCHITECTURE_OVERVIEW.md`

## RPC Failover Configuration

To reduce degraded RPC incidents, configure multiple endpoints per chain.

Axelar-related env keys (can be combined):

- `AXELAR_RPC_URL`
- `AXELAR_RPC_URLS`
- `AXELAR_RPC_BACKUP_URLS`
- `RPC_URLS_AXELAR`
- `RPC_BACKUP_URLS_AXELAR`

Generic chain format:

- `RPC_URLS_<CHAIN>`
- `RPC_BACKUP_URLS_<CHAIN>`
- `RPC_URL_<CHAIN>`

Health-check tuning:

- `RPC_STATUS_MAX_ENDPOINTS` controls endpoint fan-out in `/rpc/status/:chain`.

## CCTP Custom Tx Builder (Internal Flow)

For internal no-tab execution wiring, example settings:

```bash
STELLAR_CIRCLE_CCTP_V2_TX_API_URL=http://127.0.0.1:8085/defi/bridge/internal/circle-cctp-v2/build-tx
STELLAR_CIRCLE_CCTP_V2_UPSTREAM_TX_API_URL=http://127.0.0.1:8085/defi/bridge/internal/circle-cctp-v2/custom/build-tx
STELLAR_CIRCLE_CCTP_V2_CUSTOM_TX_BUILDER_ENABLED=true
STELLAR_CIRCLE_CCTP_V2_CUSTOM_TX_BUILDER_MODE=custom-template
STELLAR_CIRCLE_CCTP_V2_UPSTREAM_API_KEY=local-cctp-dev-key
STELLAR_CIRCLE_CCTP_V2_CUSTOM_TX_API_KEY=local-cctp-dev-key
STELLAR_CIRCLE_CCTP_V2_CUSTOM_ALLBRIDGE_API_URL=https://core.api.allbridgecoreapi.net
STELLAR_CIRCLE_CCTP_V2_CUSTOM_ALLBRIDGE_MESSENGER=ALLBRIDGE
```

Modes:

- `simulate-auto`
- `simulate-stellar-xdr`
- `simulate-evm-tx`
- `custom-template`

Important:

- Simulation modes are for integration testing only, not real bridging.
- `custom-template` availability depends on supported chains/tokens and wallet prerequisites.
- In this integration, Allbridge `custom-template` for Stellar is mainnet-only.

## Security Notes

- Do not commit `.env` or secrets.
- Keep `.env.example` non-sensitive.
- Internal bridge endpoints under `/defi/bridge/internal/*` should be network-restricted (or guarded) for production exposure.

## Troubleshooting

- `401 Unauthorized`: verify bearer token and JWT env keys.
- RPC degraded in `/rpc/status/*`: add backup RPC URLs and tune retry/timeout vars.
- Empty market/portfolio data: verify `STELLAR_NETWORK` and Horizon/RPC configuration.
- Startup DB error: verify `MONGO_URI` and MongoDB connectivity.

## Handoff Checklist

- [x] `.env.example` included
- [x] route map in `docs/API_ROUTES.md`
- [x] architecture map in `docs/ARCHITECTURE_OVERVIEW.md`
- [x] smoke scripts documented
- [x] reviewer runbook included in README
