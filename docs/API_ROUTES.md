# Yielder API Route Catalog

This catalog is derived from controller decorators in `src/modules/*/*controller.ts` on March 6, 2026.
Use this as a reviewer index. For exact request/response schemas, use Swagger at `/api/docs`.

## Base URL

- Local: `http://127.0.0.1:8085`
- Swagger UI: `GET /api/docs`

## Auth Conventions

- `Public`: no bearer token required.
- `JWT`: requires `Authorization: Bearer <token>`.
- `JWT (Admin)`: JWT required; endpoint is admin-oriented and enforced by role/permission checks in auth flows.
- `Internal`: currently exposed without JWT in code and intended for trusted internal/backend flows.

## Core and Auth

| Method | Path | Access |
|---|---|---|
| GET | `/` | Public |
| POST | `/auth/register` | Public |
| POST | `/auth/login` | Public |
| POST | `/auth/google/login` | Public |
| POST | `/auth/refresh` | Public |
| POST | `/auth/logout` | Public |
| POST | `/auth/forgot-password` | Public |
| POST | `/auth/reset-password` | Public |
| GET | `/auth/me` | JWT |
| POST | `/auth/subscription/tier` | JWT |
| POST | `/auth/subscription/tier/email` | JWT |
| GET | `/auth/admin/users` | JWT (Admin) |
| POST | `/auth/admin/role` | JWT (Admin) |
| POST | `/auth/admin/role/email` | JWT (Admin) |
| GET | `/auth/admin/permissions` | JWT |
| GET | `/auth/admin/users/:userId/details` | JWT (Admin) |
| POST | `/auth/admin/users/:userId/suspend` | JWT (Admin) |
| POST | `/auth/admin/users/:userId/unsuspend` | JWT (Admin) |
| POST | `/auth/admin/users/:userId/revoke-sessions` | JWT (Admin) |
| POST | `/auth/admin/users/:userId/password-reset` | JWT (Admin) |
| POST | `/auth/admin/users/:userId/2fa/reset` | JWT (Admin) |
| GET | `/auth/wallet/challenge` | Public |
| POST | `/auth/wallet/login` | Public |
| POST | `/auth/wallet/link` | JWT |

## Wallet and Portfolio

| Method | Path | Access |
|---|---|---|
| POST | `/wallet/connect` | JWT |
| POST | `/wallet/watch` | JWT |
| GET | `/wallet` | JWT |
| GET | `/wallet/claimable/:address` | Public |
| POST | `/wallet/claimable/build-claim-tx` | JWT |
| POST | `/wallet/withdraw` | JWT |
| POST | `/wallet/activity` | JWT |
| GET | `/wallet/:id` | JWT |
| DELETE | `/wallet/:id` | JWT |
| GET | `/portfolio` | JWT |
| GET | `/portfolio/summary` | JWT |
| GET | `/portfolio/assets` | JWT |
| GET | `/portfolio/positions` | JWT |
| GET | `/portfolio/history` | JWT |
| GET | `/portfolio/activity` | JWT |
| GET | `/portfolio/performance` | JWT |
| GET | `/portfolio/pnl` | JWT |
| GET | `/portfolio/analytics/advanced` | JWT |
| GET | `/portfolio/analytics` | JWT |

## DeFi and Platform Fee

| Method | Path | Access |
|---|---|---|
| GET | `/defi/yields` | Public |
| POST | `/defi/optimizer/plan` | JWT |
| POST | `/defi/optimizer/execute` | JWT |
| POST | `/defi/borrow/execute` | JWT |
| POST | `/defi/optimizer/:executionId/complete-deposit` | JWT |
| POST | `/defi/optimizer/:executionId/build-stellar-deposit-tx` | JWT |
| GET | `/defi/optimizer/history` | JWT |
| GET | `/defi/optimizer/fees/summary` | JWT |
| POST | `/defi/optimizer/fees/settle` | JWT |
| GET | `/defi/optimizer/fees/settlements` | JWT |
| POST | `/defi/optimizer/fees/settlements/:settlementId/confirm` | JWT |
| GET | `/defi/bridge/chains` | JWT |
| GET | `/defi/bridge/tokens` | JWT |
| GET | `/defi/bridge/quote` | JWT |
| POST | `/defi/bridge/build-tx` | JWT |
| POST | `/defi/bridge/internal/circle-cctp-v2/build-tx` | Internal |
| POST | `/defi/bridge/internal/circle-cctp-v2/custom/build-tx` | Internal |
| GET | `/defi/bridge/history` | JWT |
| POST | `/defi/bridge/history` | JWT |
| POST | `/defi/bridge/history/:bridgeTxHash/refresh` | JWT |
| GET | `/defi/swap/quote` | JWT |
| POST | `/defi/swap/build-tx` | JWT |
| GET | `/defi/platform-fees/summary` | JWT |
| GET | `/defi/platform-fees` | JWT |
| POST | `/defi/platform-fees/collect` | JWT |

## RPC and Anchor

| Method | Path | Access |
|---|---|---|
| POST | `/rpc/broadcast` | JWT |
| POST | `/rpc/call/:chain` | JWT |
| POST | `/rpc/batch/:chain` | JWT |
| GET | `/rpc/status/:chain` | JWT |
| GET | `/rpc/metrics` | JWT |
| GET | `/anchor/health` | JWT |
| GET | `/anchor/info` | JWT |
| GET | `/anchor/quote` | JWT |
| GET | `/anchor/auth/challenge` | JWT |
| POST | `/anchor/auth/token` | JWT |
| POST | `/anchor/session` | JWT |
| GET | `/anchor/transaction` | JWT |

## Market, Oracle, Realtime

| Method | Path | Access |
|---|---|---|
| GET | `/api/stellar/network-stats` | Public |
| GET | `/api/stellar/trending-assets` | Public |
| GET | `/api/stellar/liquidity-pools` | Public |
| GET | `/api/stellar/soroban-protocols` | Public |
| GET | `/api/stellar/account/:address` | Public |
| GET | `/api/stellar/defi/positions/:address` | Public |
| GET | `/oracle/prices` | Public |
| GET | `/realtime/health` | Public |

## Notifications and Billing

| Method | Path | Access |
|---|---|---|
| GET | `/notifications` | JWT |
| POST | `/notifications/read-all` | JWT |
| GET | `/notifications/admin/announcements` | JWT (Admin) |
| POST | `/notifications/admin/announcements` | JWT (Admin) |
| POST | `/notifications/admin/announcements/:announcementId/status` | JWT (Admin) |
| GET | `/notifications/admin/audit-logs` | JWT (Admin) |
| POST | `/notifications/admin/audit-logs` | JWT (Admin) |
| GET | `/notifications/admin/billing-activity` | JWT (Admin) |
| GET | `/notifications/admin/timeline` | JWT (Admin) |
| POST | `/notifications/admin/audit-logs/clear` | JWT (Admin) |
| GET | `/notifications/admin/feature-flags` | JWT (Admin) |
| POST | `/notifications/admin/feature-flags` | JWT (Admin) |
| POST | `/billing/checkout` | JWT |
| POST | `/billing/portal` | JWT |

## Reviewer Smoke Flow

1. `POST /auth/login` or `POST /auth/wallet/login` to get token.
2. `GET /auth/me` with bearer token to verify auth.
3. `GET /portfolio/summary` and `GET /wallet` for account data checks.
4. `GET /defi/yields` (public) and `GET /defi/bridge/quote` (JWT) for DeFi checks.
5. `GET /rpc/status/axelar` and `GET /realtime/health` for infra health.

## Notes

- DTO validation is enabled globally (`ValidationPipe` in `src/main.ts`).
- If you update controller routes, update this file in the same pull request.
