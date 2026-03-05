# Yielder Backend Architecture Overview

## Runtime Stack

- Framework: NestJS 10
- HTTP adapter: Fastify (`@nestjs/platform-fastify`)
- Validation: global `ValidationPipe` (whitelist + transform)
- API docs: Swagger at `/api/docs`
- Logging: Pino (`PinoLoggerService`)
- Database: MongoDB via Mongoose

Main bootstrap is in `src/main.ts`.
Root wiring is in `src/modules/app.module.ts`.

## High-Level Module Map

| Module | Responsibility |
|---|---|
| `AuthModule` | User auth, wallet auth challenge/login/link, refresh/session flows, admin user management |
| `WalletModule` | Wallet connect/watch/list/detail, claimable/withdraw/build tx, activity ingest |
| `PortfolioModule` | Portfolio summary/assets/positions/history/performance/analytics |
| `IndexerModule` | Background indexing state and chain data synchronization |
| `DeFiModule` | Yield data, optimizer/borrow flows, bridge/swap tx building, bridge history |
| `RpcModule` | Multi-endpoint RPC calls, broadcast, status/metrics, Axelar client integration |
| `AnchorModule` | Anchor quote/session/transaction integration endpoints |
| `MarketModule` | Public market/network/trending/liquidity/soroban/account views |
| `OracleModule` | Price aggregation endpoint |
| `RealtimeModule` | Realtime health/state endpoint and periodic refresh layer |
| `NotificationsModule` | User notifications + admin announcements/audit/billing activity/flags |
| `BillingModule` | Checkout + customer portal handoff endpoints |
| `AccessModule` | Access-control helper services used by secured modules |
| `PlatformFeeModule` | Platform fee record handling used in DeFi/notifications flows |
| `SharedModule` | Shared logger and cross-cutting utilities |

## Data Model (MongoDB)

Primary schema groups:

- Auth: `User`, `RefreshToken`, `PasswordReset`, `AuthChallenge`, `ConnectedWallet`
- Wallet/Portfolio: `Wallet`, `Transaction`, `PortfolioSnapshot`
- DeFi: `DeFiPosition`, `BridgeHistory`, `OptimizerExecution`, `OptimizerFeeSettlement`
- Indexer: `IndexerState`
- Notifications/Admin: `Announcement`, `AdminAuditEvent`, `AdminFeatureFlags`
- Billing/Fee tracking: `PlatformFeeRecord`

Schema files live under `src/modules/*/schemas`.

## Request Security Model

- JWT-protected routes use `JwtAuthGuard`.
- Admin routes are under `/auth/admin/*` and `/notifications/admin/*` and rely on role/permission checks.
- Bearer auth is included in Swagger config.
- CORS is configurable via `CORS_ORIGIN`.

Important hardening note:
- `/defi/bridge/internal/circle-cctp-v2/*` routes are intended for internal workflows. They are currently not JWT-guarded in controller code; expose this service only behind trusted network boundaries or add guarding before internet-facing deployment.

## Environment Configuration

- Required baseline env is documented in `.env.example`.
- Most critical keys for boot:
  - `MONGO_URI`
  - `JWT_SECRET`
  - `JWT_REFRESH_SECRET`
  - `PORT`
  - `STELLAR_NETWORK` and Stellar Horizon URLs
- Performance/reliability knobs:
  - RPC failover keys (`AXELAR_RPC_URLS`, backup URLs, retry/timeouts)
  - market/oracle timeout knobs

## Local Run and Review

1. `npm install`
2. `cp .env.example .env`
3. `npm run start:dev`
4. open Swagger at `http://127.0.0.1:8085/api/docs`
5. run smoke scripts:
   - `npm run smoke:wallet:evm`
   - `npm run smoke:bridge:evm`

## Handoff Checklist for External Reviewers

- Code access to `main` branch
- `.env.example` present and up to date
- Route map in `docs/API_ROUTES.md`
- Architecture map in this document
- Smoke scripts runnable locally

