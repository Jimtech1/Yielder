const axios = require('axios');
const { ethers } = require('ethers');

const DEFAULT_PORT = process.env.PORT || '8085';
const BASE_URL = (process.env.API_BASE_URL || `http://127.0.0.1:${DEFAULT_PORT}`).replace(/\/+$/, '');

const SRC_CHAIN = (process.env.BRIDGE_SRC_CHAIN || 'ethereum').trim().toLowerCase();
const DST_CHAIN = (process.env.BRIDGE_DST_CHAIN || 'arbitrum').trim().toLowerCase();
const SRC_SYMBOL = (process.env.BRIDGE_SRC_SYMBOL || 'USDC').trim().toUpperCase();
const DST_SYMBOL = (process.env.BRIDGE_DST_SYMBOL || SRC_SYMBOL).trim().toUpperCase();
const SRC_AMOUNT = (process.env.BRIDGE_SRC_AMOUNT || '1').trim();

function fail(message, details) {
  console.error(`❌ ${message}`);
  if (details) {
    console.error(details);
  }
  process.exit(1);
}

function parseAxiosError(error) {
  if (error.response) {
    return {
      status: error.response.status,
      data: error.response.data,
    };
  }
  return {
    status: null,
    data: error.message,
  };
}

async function authenticateEvmWallet() {
  const wallet = ethers.Wallet.createRandom();
  const address = wallet.address;

  console.log(`🔑 EVM wallet: ${address}`);
  console.log(`🌐 API base: ${BASE_URL}`);

  const challengeRes = await axios.get(`${BASE_URL}/auth/wallet/challenge`, {
    params: { publicKey: address },
  });

  if (!challengeRes.data || !challengeRes.data.challenge) {
    fail('Challenge endpoint did not return challenge payload', challengeRes.data);
  }

  const signature = await wallet.signMessage(challengeRes.data.challenge);

  const loginRes = await axios.post(`${BASE_URL}/auth/wallet/login`, {
    publicKey: address,
    signature,
  });

  const accessToken = loginRes.data?.accessToken;
  if (!accessToken) {
    fail('Wallet login did not return an accessToken', loginRes.data);
  }

  console.log('✅ Wallet auth succeeded');
  return { accessToken, address };
}

async function fetchTokenForChain(chainKey, symbol, headers) {
  const response = await axios.get(`${BASE_URL}/defi/bridge/tokens`, {
    params: {
      chainKey,
      bridgeableOnly: 'true',
    },
    headers,
  });

  const tokens = Array.isArray(response.data) ? response.data : [];
  if (tokens.length === 0) {
    fail(`No bridgeable tokens returned for chain "${chainKey}"`);
  }

  const exactSymbol = tokens.find(
    (token) => String(token.symbol || '').trim().toUpperCase() === symbol,
  );

  if (exactSymbol) {
    return exactSymbol;
  }

  console.warn(
    `⚠️  Symbol ${symbol} not found on ${chainKey}. Falling back to first token (${tokens[0].symbol}).`,
  );
  return tokens[0];
}

async function run() {
  try {
    const { accessToken, address } = await authenticateEvmWallet();
    const headers = { Authorization: `Bearer ${accessToken}` };

    const [srcToken, dstToken] = await Promise.all([
      fetchTokenForChain(SRC_CHAIN, SRC_SYMBOL, headers),
      fetchTokenForChain(DST_CHAIN, DST_SYMBOL, headers),
    ]);

    console.log(`🧭 Route request: ${SRC_CHAIN}:${srcToken.symbol} -> ${DST_CHAIN}:${dstToken.symbol}`);

    const quoteRes = await axios.get(`${BASE_URL}/defi/bridge/quote`, {
      params: {
        srcChainKey: SRC_CHAIN,
        dstChainKey: DST_CHAIN,
        srcToken: srcToken.address,
        dstToken: dstToken.address,
        srcAddress: address,
        dstAddress: address,
        srcAmount: SRC_AMOUNT,
        slippageBps: 100,
      },
      headers,
    });

    const quote = quoteRes.data;
    const routes = Array.isArray(quote?.quotes) ? quote.quotes : [];

    console.log(`📈 routeCount=${quote?.routeCount ?? routes.length} recommended=${quote?.recommendedRoute || 'n/a'}`);

    if (routes.length === 0) {
      fail('Bridge quote returned zero routes for this pair/amount', quote);
    }

    const selectedRoute =
      quote.recommendedRoute ||
      routes.find((route) => route?.transactions?.bridge)?.route ||
      routes[0]?.route;

    if (!selectedRoute) {
      fail('Unable to select a bridge route from quote response', quote);
    }

    const buildRes = await axios.post(
      `${BASE_URL}/defi/bridge/build-tx`,
      {
        srcChainKey: SRC_CHAIN,
        dstChainKey: DST_CHAIN,
        srcToken: srcToken.address,
        dstToken: dstToken.address,
        srcAddress: address,
        dstAddress: address,
        srcAmount: SRC_AMOUNT,
        slippageBps: 100,
        route: selectedRoute,
      },
      { headers },
    );

    const build = buildRes.data;
    if (!build || typeof build !== 'object') {
      fail('Bridge build response is empty or malformed', build);
    }

    const hasExecutablePayload =
      build.executionType === 'evm'
        ? Boolean(build.bridgeTransaction || build.bridgeTxHash)
        : Boolean(build.externalUrl);

    if (!hasExecutablePayload) {
      fail('Bridge build response has no executable payload', build);
    }

    console.log(`✅ Bridge build succeeded (executionType=${build.executionType}, route=${build.route})`);
    if (build.bridgeTransaction?.to) {
      console.log(`   bridge tx to: ${build.bridgeTransaction.to}`);
    }
    if (build.bridgeTxHash) {
      console.log(`   backend tx hash: ${build.bridgeTxHash}`);
    }
  } catch (error) {
    const parsed = parseAxiosError(error);
    fail(`Request failed${parsed.status ? ` (status ${parsed.status})` : ''}`, parsed.data);
  }
}

run();
