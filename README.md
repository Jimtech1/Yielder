# Yielder Backend

## Reviewer Quick Start

1. Install dependencies.

```bash
npm install
```

2. Create local env file.

```bash
cp .env.example .env
```

3. Start API locally.

```bash
npm run start:dev
```

Default API base URL is `http://127.0.0.1:8085`.

4. Run smoke checks.

```bash
npm run smoke:wallet:evm
npm run smoke:bridge:evm
```

Optional env vars for bridge smoke:
- `BRIDGE_SRC_CHAIN` (default `ethereum`)
- `BRIDGE_DST_CHAIN` (default `arbitrum`)
- `BRIDGE_SRC_SYMBOL` (default `USDC`)
- `BRIDGE_DST_SYMBOL` (default `USDC`)
- `BRIDGE_SRC_AMOUNT` (default `1`)

Notes
- `/defi/bridge/*` supports EVM route quote/build (and Stellar external routes).
- `/defi/swap/*` is Stellar path-payment swap, not EVM swap.

## Documentation

- API route catalog: `docs/API_ROUTES.md`
- Architecture overview: `docs/ARCHITECTURE_OVERVIEW.md`
- Interactive API docs at runtime: `GET /api/docs`

RPC backup/failover configuration

To avoid `RPC: Degraded` when a provider is rate-limited, configure multiple endpoints per chain.

Axelar-supported env keys (all can be used together):
- `AXELAR_RPC_URL` (single primary endpoint)
- `AXELAR_RPC_URLS` (comma-separated list)
- `AXELAR_RPC_BACKUP_URLS` (comma-separated backup list)
- `RPC_URLS_AXELAR` (generic chain key format)
- `RPC_BACKUP_URLS_AXELAR` (generic backup list)

Generic chain format:
- `RPC_URLS_<CHAIN>`
- `RPC_BACKUP_URLS_<CHAIN>`
- `RPC_URL_<CHAIN>` (legacy single endpoint)

Health-check tuning:
- `RPC_STATUS_MAX_ENDPOINTS` controls how many configured endpoints are checked for `/rpc/status/:chain`.

Custom Stellar CCTP v2 tx-builder scaffold

For no-tab in-app execution without a partner endpoint, you can wire the internal upstream to the custom scaffold endpoint:

```bash
STELLAR_CIRCLE_CCTP_V2_TX_API_URL=http://127.0.0.1:8085/defi/bridge/internal/circle-cctp-v2/build-tx
STELLAR_CIRCLE_CCTP_V2_UPSTREAM_TX_API_URL=http://127.0.0.1:8085/defi/bridge/internal/circle-cctp-v2/custom/build-tx
STELLAR_CIRCLE_CCTP_V2_CUSTOM_TX_BUILDER_ENABLED=true
STELLAR_CIRCLE_CCTP_V2_CUSTOM_TX_BUILDER_MODE=custom-template
STELLAR_CIRCLE_CCTP_V2_UPSTREAM_API_KEY=local-cctp-dev-key
STELLAR_CIRCLE_CCTP_V2_CUSTOM_TX_API_KEY=local-cctp-dev-key
STELLAR_CIRCLE_CCTP_V2_CUSTOM_ALLBRIDGE_API_URL=https://core.api.allbridgecoreapi.net
STELLAR_CIRCLE_CCTP_V2_CUSTOM_ALLBRIDGE_MESSENGER=ALLBRIDGE
STELLAR_CIRCLE_CCTP_V2_CUSTOM_ALLBRIDGE_CHAIN_SYMBOL_OVERRIDES='{"blast":"BLA"}'
```

Modes:
- `simulate-auto`: Stellar source returns `bridgeStellarTransaction` XDR; EVM source returns a no-op EVM tx.
- `simulate-stellar-xdr`: always builds Stellar payment XDR (requires `srcChainKey=stellar`).
- `simulate-evm-tx`: always builds no-op EVM transaction payload.
- `custom-template`: real provider integration via Allbridge Core REST API (`/chains`, `/check/bridge/allowance`, `/raw/bridge/approve`, `/raw/bridge`).

Important:
- simulation modes are for end-to-end in-app wiring tests only (not real cross-chain bridging).
- `custom-template` is real provider flow, but route availability and success depend on Allbridge-supported chains/tokens and destination wallet prerequisites (for Stellar, trustline requirements may still apply).
- In this integration, Allbridge `custom-template` mode is mainnet-only for Stellar. If `STELLAR_NETWORK=testnet`, in-app Circle CCTP v2 custom-template routes are rejected by design.
- use `STELLAR_CIRCLE_CCTP_V2_CUSTOM_ALLBRIDGE_CHAIN_SYMBOL_OVERRIDES` when your local chain keys differ from Allbridge chain symbols.
