
require('dotenv').config({ path: './.env' });
const crypto = require('crypto');

const fetchFn = global.fetch || ((...args) =>
    import('node-fetch').then(({ default: fetch }) => fetch(...args))
);

// --- CONFIGURATION ---
const NGROK_URL = 'https://94c4d018290c.ngrok-free.app';
const ENDPOINT = '/session-closed';
const HMAC_SECRET = process.env.MATCHMAKING_HMAC_SECRET;

// --- DATA TO SEND ---
const payload = {
    session_id: "c65eb045-d7da-4638-900d-f9293c64cbde",
};

// --- SCRIPT LOGIC ---

async function sendSignedWebhook() {
    if (!HMAC_SECRET) {
        console.error('FATAL: Could not find MATCHMAKING_HMAC_SECRET in your .env file.');
        return;
    }

    const url = `${NGROK_URL}${ENDPOINT}`;
    const body = JSON.stringify(payload);

    const signature = crypto.createHmac('sha256', HMAC_SECRET)
                            .update(body)
                            .digest('hex');
    
    console.log(`Sending webhook to: ${url}`);
    console.log(`Payload body: ${body}`);
    console.log(`Calculated Signature: ${signature}`);

    try {
        const response = await fetchFn(url, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'X-Signature': signature // Use the correct header for game-server spec
            },
            body: body
        });

        const responseBody = await response.text();
        console.log('\n--- RESPONSE ---');
        console.log(`Status: ${response.status} ${response.statusText}`);
        console.log(`Body: ${responseBody}`);

    } catch (error) {
        console.error('\n--- ERROR ---');
        console.error('Failed to send webhook:', error.message);
    }
}

sendSignedWebhook();
