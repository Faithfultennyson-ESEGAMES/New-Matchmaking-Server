require('dotenv').config({ path: require('path').resolve(__dirname, '.env') });
const crypto = require('crypto');

const fetchFn = global.fetch || ((...args) =>
  import('node-fetch').then(({ default: fetch }) => fetch(...args))
);

const baseUrl =
  process.env.MATCHMAKING_SERVICE_URL ||
  process.env.MATCHMAKING_SERVER_URL ||
  process.env.MATCHMAKING_URL;
const HMAC_SECRET = process.env.HMAC_SECRET;

if (!baseUrl) {
  console.error('Missing MATCHMAKING_SERVICE_URL (or MATCHMAKING_SERVER_URL/MATCHMAKING_URL) in env.');
  process.exit(1);
}

if (!HMAC_SECRET) {
  console.error('Missing HMAC_SECRET in env.');
  process.exit(1);
}

const url = /\/session-closed\/?$/.test(baseUrl)
  ? baseUrl
  : `${baseUrl.replace(/\/+$/, '')}/session-closed`;

const payload = {
  sessionId: process.env.TEST_SESSION_ID || 'test-session-closed-1',
  status: 'ended',
  endedAt: new Date().toISOString()
};

const body = JSON.stringify(payload);
const signature = crypto.createHmac('sha256', HMAC_SECRET).update(body).digest('hex');

async function sendSessionClosed() {
  console.log(`Sending session-closed to: ${url}`);
  console.log(`Payload: ${body}`);
  console.log(`Signature: ${signature}`);

  try {
    const response = await fetchFn(url, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-Hub-Signature-256': signature
      },
      body
    });

    const responseBody = await response.text();
    console.log(`Response: ${response.status} ${response.statusText}`);
    console.log(`Body: ${responseBody}`);
  } catch (error) {
    console.error('Request failed:', error.message);
    process.exit(1);
  }
}

sendSessionClosed();
