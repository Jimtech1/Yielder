const axios = require('axios');
const StellarSdk = require('stellar-sdk');

const DEFAULT_PORT = process.env.PORT || '8085';
const BASE_URL = (process.env.API_BASE_URL || `http://127.0.0.1:${DEFAULT_PORT}`).replace(/\/+$/, '');

async function testAuthFlow() {
  try {
    // 1. Generate Keypair
    const pair = StellarSdk.Keypair.random();
    const publicKey = pair.publicKey();
    console.log(`🔑 Generated Key: ${publicKey}`);

    // 2. Request Challenge
    console.log(`\n1. Requesting Challenge...`);
    const challengeRes = await axios.get(`${BASE_URL}/auth/wallet/challenge`, {
      params: { publicKey }
    });

    if (!challengeRes.data || !challengeRes.data.challenge) {
      console.error('❌ Failed: No challenge data returned');
      return;
    }

    const { challenge } = challengeRes.data;
    console.log(`✅ Challenge Received: "${challenge.substring(0, 30)}..."`);

    // 3. Sign Challenge
    // Note: The message to sign matches exactly what is in the challenge string
    const signature = pair.sign(Buffer.from(challenge)).toString('base64');
    console.log(`✍️ Signed Challenge.`);

    // 4. Login
    console.log(`\n2. Logging in...`);
    const loginRes = await axios.post(`${BASE_URL}/auth/wallet/login`, {
      publicKey,
      signature
    });

    console.log('Login Response Status:', loginRes.status);
    console.log('Login Result:', loginRes.data);

    if (loginRes.data.accessToken) {
        console.log('✅ Login Successful! Access Token received.');
    } else {
        console.error('❌ Login Failed: No token.');
    }

  } catch (error) {
    console.error('❌ Request Failed:');
    if (error.response) {
      console.error('Status:', error.response.status);
      console.error('Data:', error.response.data);
    } else {
      console.error(error.message);
    }
  }
}

testAuthFlow();
