require('dotenv').config();
const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const crypto = require('crypto');
const path = require('path');
const { JSONFile, Low } = require('lowdb');
const cors = require('cors');

const PORT = process.env.PORT || 3330;
const DLQ_PASSWORD = process.env.DLQ_PASSWORD;
const HMAC_SECRET = process.env.HMAC_SECRET;
const GAME_SERVER_URL = process.env.GAME_SERVER_URL;
const DICE_GAME_SERVER_URL = process.env.DICE_GAME_SERVER_URL || GAME_SERVER_URL;
const TICTACTOE_GAME_SERVER_URL = process.env.TICTACTOE_GAME_SERVER_URL || GAME_SERVER_URL;
const OPENBOX_GAME_SERVER_URL = process.env.OPENBOX_GAME_SERVER_URL || '';
const OPENBOX_CLIENT_URL = process.env.OPENBOX_CLIENT_URL || '';
const OPENBOX_CONTROL_TOKEN = process.env.OPENBOX_CONTROL_TOKEN || '';
const DICE_TURN_TIME_MS = parseInt(process.env.DICE_TURN_TIME_MS, 10) || 8000;
const TICTACTOE_TURN_DURATION_SEC = parseInt(process.env.TICTACTOE_TURN_DURATION_SEC, 10) || 6;
const DB_ENTRY_TTL_MS = parseInt(process.env.DB_ENTRY_TTL_MS, 10) || 3600000;
const MAX_SESSION_CREATION_ATTEMPTS = parseInt(process.env.MAX_SESSION_CREATION_ATTEMPTS, 10) || 3;
const SESSION_CREATION_RETRY_DELAY_MS = parseInt(process.env.SESSION_CREATION_RETRY_DELAY_MS, 10) || 1500;
const CANCEL_JOIN_WINDOW_MS = parseInt(process.env.CANCEL_JOIN_WINDOW_MS, 10) || 300000;
const MAX_CANCEL_JOIN = parseInt(process.env.MAX_CANCEL_JOIN, 10) || 8;
const COOLDOWN_MS = parseInt(process.env.COOLDOWN_MS, 10) || 60000;
const DICE_MODES = [2, 4, 6, 15];

const parseCsvInts = (value, fallback = []) => {
    const parsed = String(value || '')
        .split(',')
        .map((entry) => parseInt(entry.trim(), 10))
        .filter((entry) => Number.isFinite(entry) && entry > 0);
    return parsed.length ? parsed : fallback;
};

const OPENBOX_STAKE_AMOUNTS = parseCsvInts(process.env.OPENBOX_STAKE_AMOUNTS, [100, 500, 1000]);
const OPENBOX_PLAYER_COUNTS = parseCsvInts(process.env.OPENBOX_PLAYER_COUNTS, [5, 10, 20]);
const OPENBOX_MIN_REPLAY_PLAYERS = 5;

if (!HMAC_SECRET) {
    console.error('FATAL ERROR: HMAC_SECRET must be defined in .env file.');
    process.exit(1);
}

function validateGameServerConfig(gameType) {
    if (gameType === 'dice') {
        if (!DICE_GAME_SERVER_URL) {
            return { ok: false, message: 'DICE_GAME_SERVER_URL is not configured.' };
        }
        if (!DLQ_PASSWORD) {
            return { ok: false, message: 'DLQ_PASSWORD is not configured for Dice session creation.' };
        }
        return { ok: true };
    }

    if (gameType === 'tictactoe') {
        if (!TICTACTOE_GAME_SERVER_URL) {
            return { ok: false, message: 'TICTACTOE_GAME_SERVER_URL is not configured.' };
        }
        if (!DLQ_PASSWORD) {
            return { ok: false, message: 'DLQ_PASSWORD is not configured for TicTacToe session creation.' };
        }
        return { ok: true };
    }

    if (!OPENBOX_GAME_SERVER_URL) {
        return { ok: false, message: 'OPENBOX_GAME_SERVER_URL is not configured.' };
    }
    if (!OPENBOX_CONTROL_TOKEN) {
        return { ok: false, message: 'OPENBOX_CONTROL_TOKEN is not configured.' };
    }
    return { ok: true };
}

const app = express();
const server = http.createServer(app);

app.use(cors());
app.use('/test-client', express.static(path.join(__dirname, 'test-client')));
app.use('/openbox-app', express.static(path.join(__dirname, 'openbox-app')));
app.use('/socket.io-client', express.static(path.join(__dirname, 'node_modules', 'socket.io', 'client-dist')));

const saveRawBody = (req, res, buf, encoding) => {
    if (buf && buf.length) {
        req.rawBody = buf.toString(encoding || 'utf8');
    }
};
app.use(express.json({ verify: saveRawBody }));

const io = new Server(server, {
    cors: {
        origin: '*',
        methods: ['GET', 'POST']
    }
});

const verifyWebhookSignature = (req, res, next) => {
    const signature = req.headers['x-hub-signature-256'];
    if (!signature) {
        console.error("[Webhook Error] Signature header missing. Expected 'x-hub-signature-256'.");
        return res.status(401).send("Signature header missing. Expected 'x-hub-signature-256'.");
    }

    if (!req.rawBody) {
        console.error('[Webhook Error] Raw body not available for signature verification.');
        return res.status(500).send('Internal Server Error: Raw body not saved.');
    }

    const expectedSignature = crypto.createHmac('sha256', HMAC_SECRET).update(req.rawBody).digest('hex');
    if (!crypto.timingSafeEqual(Buffer.from(signature), Buffer.from(expectedSignature))) {
        console.error('[Webhook Error] Invalid signature.');
        return res.status(403).send('Invalid signature.');
    }

    next();
};

const adapter = new JSONFile('db.json');
const db = new Low(adapter);
const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const matchingLocks = new Set();

function normalizeGameType(raw) {
    const value = (raw || '').toString().trim().toLowerCase();
    if (value === 'dice') return 'dice';
    if (value === 'tictactoe' || value === 'tic-tac-toe' || value === 'ttt') return 'tictactoe';
    if (value === 'openbox' || value === 'open-box' || value === 'open_box') return 'openbox';
    return null;
}

function normalizeMode(gameType, mode) {
    if (gameType === 'tictactoe') return 2;
    const parsed = parseInt(mode, 10);
    if (!Number.isFinite(parsed)) return null;
    return DICE_MODES.includes(parsed) ? parsed : null;
}

function normalizeOpenBoxStake(stakeAmount) {
    const parsed = parseInt(stakeAmount, 10);
    if (!Number.isFinite(parsed)) return null;
    return OPENBOX_STAKE_AMOUNTS.includes(parsed) ? parsed : null;
}

function normalizeOpenBoxPlayerCount(playerCount) {
    const parsed = parseInt(playerCount, 10);
    if (!Number.isFinite(parsed)) return null;
    return OPENBOX_PLAYER_COUNTS.includes(parsed) ? parsed : null;
}

function normalizeBaseUrl(value) {
    return String(value || '').trim().replace(/\/+$/, '');
}

function ensureQueueStructure(data) {
    if (!data.queue || Array.isArray(data.queue)) {
        const legacyQueue = Array.isArray(data.queue) ? data.queue : [];
        data.queue = {
            dice: { '2': [], '4': [], '6': [], '15': [] },
            tictactoe: { '2': [] },
            openbox: {}
        };
        if (legacyQueue.length) {
            data.queue.tictactoe['2'] = legacyQueue;
        }
    }

    data.queue.dice = data.queue.dice || {};
    data.queue.tictactoe = data.queue.tictactoe || {};
    data.queue.openbox = data.queue.openbox || {};

    for (const mode of DICE_MODES) {
        data.queue.dice[String(mode)] = data.queue.dice[String(mode)] || [];
    }
    data.queue.tictactoe['2'] = data.queue.tictactoe['2'] || [];

    for (const playerCount of OPENBOX_PLAYER_COUNTS) {
        const playerCountKey = String(playerCount);
        data.queue.openbox[playerCountKey] = data.queue.openbox[playerCountKey] || {};
        for (const stakeAmount of OPENBOX_STAKE_AMOUNTS) {
            const stakeKey = String(stakeAmount);
            data.queue.openbox[playerCountKey][stakeKey] = data.queue.openbox[playerCountKey][stakeKey] || [];
        }
    }

    data.active_games = data.active_games || {};
    data.active_sessions = data.active_sessions || {};
    data.ended_games = data.ended_games || {};
    data.rate_limit = data.rate_limit || {};
}

async function initializeDatabase() {
    await db.read();
    db.data = db.data || {};
    ensureQueueStructure(db.data);
    await db.write();
}

function buildQueueStatus(data) {
    ensureQueueStructure(data);

    const dice = {};
    for (const mode of DICE_MODES) {
        dice[mode] = data.queue.dice[String(mode)]?.length || 0;
    }

    const tictactoe = {
        2: data.queue.tictactoe['2']?.length || 0
    };

    const openbox = {};
    for (const playerCount of OPENBOX_PLAYER_COUNTS) {
        const playerCountKey = String(playerCount);
        openbox[playerCount] = {};
        for (const stakeAmount of OPENBOX_STAKE_AMOUNTS) {
            const stakeKey = String(stakeAmount);
            openbox[playerCount][stakeAmount] = data.queue.openbox[playerCountKey]?.[stakeKey]?.length || 0;
        }
    }

    return { dice, tictactoe, openbox };
}

function broadcastQueueStatus() {
    io.emit('queue-status', buildQueueStatus(db.data));
}

function getQueueBucket(data, gameType, options = {}) {
    ensureQueueStructure(data);
    if (gameType === 'dice') {
        return data.queue.dice[String(options.mode)] || [];
    }
    if (gameType === 'tictactoe') {
        return data.queue.tictactoe['2'] || [];
    }
    if (gameType === 'openbox') {
        const playerCountKey = String(options.playerCount);
        const stakeKey = String(options.stakeAmount);
        data.queue.openbox[playerCountKey] = data.queue.openbox[playerCountKey] || {};
        data.queue.openbox[playerCountKey][stakeKey] = data.queue.openbox[playerCountKey][stakeKey] || [];
        return data.queue.openbox[playerCountKey][stakeKey];
    }
    return [];
}

function getQueueBuckets(data) {
    ensureQueueStructure(data);
    const buckets = [
        ...Object.values(data.queue.dice || {}),
        ...Object.values(data.queue.tictactoe || {})
    ];

    for (const playerCountBucket of Object.values(data.queue.openbox || {})) {
        buckets.push(...Object.values(playerCountBucket || {}));
    }

    return buckets;
}

function removeFromQueues(data, playerId) {
    let removed = false;
    for (const bucket of getQueueBuckets(data)) {
        let index = bucket.findIndex((entry) => entry.playerId === playerId);
        while (index !== -1) {
            bucket.splice(index, 1);
            removed = true;
            index = bucket.findIndex((entry) => entry.playerId === playerId);
        }
    }
    return removed;
}

function upsertRateLimit(data, playerId) {
    ensureQueueStructure(data);
    const now = Date.now();
    const entry = data.rate_limit[playerId] || { count: 0, windowStart: now, cooldownUntil: 0 };
    if (now - entry.windowStart > CANCEL_JOIN_WINDOW_MS) {
        entry.count = 0;
        entry.windowStart = now;
    }
    data.rate_limit[playerId] = entry;
    return entry;
}

function registerQueueAction(data, playerId, { blockOnLimit }) {
    const entry = upsertRateLimit(data, playerId);
    const now = Date.now();
    if (entry.cooldownUntil && now < entry.cooldownUntil) {
        return { blocked: true, cooldownUntil: entry.cooldownUntil };
    }

    entry.count += 1;
    if (entry.count > MAX_CANCEL_JOIN) {
        entry.cooldownUntil = now + COOLDOWN_MS;
        entry.count = 0;
        entry.windowStart = now;
        return blockOnLimit ? { blocked: true, cooldownUntil: entry.cooldownUntil } : { blocked: false };
    }

    return { blocked: false };
}

function getMatchmakingKey(gameType, options) {
    if (gameType === 'dice') return `dice:${options.mode}`;
    if (gameType === 'tictactoe') return 'tictactoe:2';
    return `openbox:${options.playerCount}:${options.stakeAmount}`;
}

function resolveRequestOptions(gameType, payload) {
    if (gameType === 'dice') {
        const mode = normalizeMode(gameType, payload.mode);
        if (!mode) {
            return { error: 'Invalid mode. Dice modes: 2, 4, 6, 15.' };
        }
        return {
            requiredPlayers: mode,
            mode,
            playerCount: mode
        };
    }

    if (gameType === 'tictactoe') {
        return {
            requiredPlayers: 2,
            mode: 2,
            playerCount: 2
        };
    }

    const playerCount = normalizeOpenBoxPlayerCount(payload.playerCount ?? payload.mode);
    if (!playerCount) {
        return { error: `Invalid playerCount. Open Box player counts: ${OPENBOX_PLAYER_COUNTS.join(', ')}.` };
    }

    const stakeAmount = normalizeOpenBoxStake(payload.stakeAmount);
    if (!stakeAmount) {
        return { error: `Invalid stakeAmount. Open Box stake amounts: ${OPENBOX_STAKE_AMOUNTS.join(', ')}.` };
    }

    return {
        requiredPlayers: playerCount,
        playerCount,
        stakeAmount
    };
}

function isPlayerConnected(playerId) {
    const room = io.sockets.adapter.rooms.get(playerId);
    return !!room && room.size > 0;
}

function buildOpenBoxWsUrl(baseUrl) {
    try {
        const url = new URL(baseUrl);
        url.protocol = url.protocol === 'https:' ? 'wss:' : 'ws:';
        return url.origin;
    } catch {
        return '';
    }
}

function buildOpenBoxLaunchUrl(joinUrl, sessionId, playerId, playerName) {
    const clientBaseUrl = normalizeBaseUrl(OPENBOX_CLIENT_URL);
    if (clientBaseUrl) {
        const params = new URLSearchParams({
            joinUrl,
            sessionId,
            playerId,
            playerName
        });

        const wsUrl = buildOpenBoxWsUrl(OPENBOX_GAME_SERVER_URL);
        if (wsUrl) {
            params.set('ws', wsUrl);
        }

        return `${clientBaseUrl}${clientBaseUrl.includes('?') ? '&' : '?'}${params.toString()}`;
    }

    try {
        const url = new URL(joinUrl);
        url.searchParams.set('playerId', playerId);
        url.searchParams.set('playerName', playerName);
        return url.toString();
    } catch {
        const separator = joinUrl.includes('?') ? '&' : '?';
        return `${joinUrl}${separator}playerId=${encodeURIComponent(playerId)}&playerName=${encodeURIComponent(playerName)}`;
    }
}

function buildMatchFoundPayload(activeGame) {
    return {
        sessionId: activeGame.sessionId,
        joinUrl: activeGame.joinUrl,
        launchUrl: activeGame.launchUrl || activeGame.joinUrl,
        gameType: activeGame.gameType,
        mode: activeGame.mode ?? null,
        stakeAmount: activeGame.stakeAmount ?? null,
        playerCount: activeGame.playerCount ?? activeGame.mode ?? null
    };
}

function buildReplayStatusPayload(activeSession) {
    const connectedEligiblePlayerIds = activeSession.players
        .map((player) => player.playerId)
        .filter((playerId) => isPlayerConnected(playerId));

    return {
        sessionId: activeSession.sessionId,
        gameType: activeSession.gameType,
        status: activeSession.status,
        playerCount: activeSession.playerCount ?? activeSession.mode ?? null,
        stakeAmount: activeSession.stakeAmount ?? null,
        roundId: activeSession.roundId || null,
        roundNumber: activeSession.roundNumber || null,
        replayReadyCount: activeSession.replayReadyPlayerIds.length,
        replayReadyPlayerIds: [...activeSession.replayReadyPlayerIds],
        replayDeclinedPlayerIds: [...activeSession.replayDeclinedPlayerIds],
        connectedEligibleCount: connectedEligiblePlayerIds.length,
        connectedEligiblePlayerIds,
        canReplay: connectedEligiblePlayerIds.length >= OPENBOX_MIN_REPLAY_PLAYERS,
        minReplayPlayers: OPENBOX_MIN_REPLAY_PLAYERS,
        launchUrlByPlayerId: Object.fromEntries(
            activeSession.players.map((player) => [
                player.playerId,
                buildOpenBoxLaunchUrl(activeSession.joinUrl, activeSession.sessionId, player.playerId, player.playerName)
            ])
        )
    };
}

function buildActiveSessionRecord(gameType, options, sessionResult, candidates) {
    return {
        sessionId: sessionResult.sessionId,
        joinUrl: sessionResult.joinUrl,
        gameType,
        mode: options.mode ?? null,
        playerCount: options.playerCount ?? options.mode ?? null,
        stakeAmount: options.stakeAmount ?? null,
        status: gameType === 'openbox' ? 'waitingForPlayers' : 'active',
        players: candidates.map((entry) => ({
            playerId: entry.playerId,
            playerName: entry.playerName,
            socketId: entry.socketId,
            joinedQueueAt: entry.queuedAt || new Date().toISOString()
        })),
        replayReadyPlayerIds: [],
        replayDeclinedPlayerIds: [],
        roundId: null,
        roundNumber: null,
        replayStartedAt: null,
        lastEventType: null,
        createdAt: new Date().toISOString(),
        updatedAt: new Date().toISOString()
    };
}

function buildActiveGameRecord(activeSession, player) {
    return {
        sessionId: activeSession.sessionId,
        joinUrl: activeSession.joinUrl,
        launchUrl:
            activeSession.gameType === 'openbox'
                ? buildOpenBoxLaunchUrl(activeSession.joinUrl, activeSession.sessionId, player.playerId, player.playerName)
                : activeSession.joinUrl,
        gameType: activeSession.gameType,
        mode: activeSession.mode ?? null,
        playerCount: activeSession.playerCount ?? activeSession.mode ?? null,
        stakeAmount: activeSession.stakeAmount ?? null,
        players: activeSession.players.map((entry) => ({
            playerId: entry.playerId,
            playerName: entry.playerName
        })),
        createdAt: activeSession.createdAt,
        updatedAt: new Date().toISOString()
    };
}

function syncActiveGamesForSession(data, activeSession) {
    activeSession.updatedAt = new Date().toISOString();
    data.active_sessions[activeSession.sessionId] = activeSession;

    const livePlayerIds = new Set(activeSession.players.map((player) => player.playerId));
    for (const [playerId, game] of Object.entries(data.active_games)) {
        if (game.sessionId === activeSession.sessionId && !livePlayerIds.has(playerId)) {
            delete data.active_games[playerId];
        }
    }

    for (const player of activeSession.players) {
        data.active_games[player.playerId] = buildActiveGameRecord(activeSession, player);
    }
}

function emitToPlayers(playerIds, eventName, payload) {
    for (const playerId of playerIds) {
        io.to(playerId).emit(eventName, payload);
    }
}

function getSessionPlayerIds(data, sessionId) {
    const activeSession = data.active_sessions[sessionId];
    if (activeSession) {
        return activeSession.players.map((player) => player.playerId);
    }

    return Object.keys(data.active_games).filter((playerId) => data.active_games[playerId]?.sessionId === sessionId);
}

function clearActiveSession(data, sessionId, reason, eventName = 'session-ended') {
    const playerIds = getSessionPlayerIds(data, sessionId);
    const activeSession = data.active_sessions[sessionId] || null;
    const gameType = activeSession?.gameType || data.active_games[playerIds[0]]?.gameType || null;
    const payload = { sessionId, reason, gameType };

    if (eventName) {
        emitToPlayers(playerIds, eventName, payload);
    }
    if (eventName !== 'session-ended') {
        emitToPlayers(playerIds, 'session-ended', payload);
    }

    for (const playerId of playerIds) {
        delete data.active_games[playerId];
    }

    delete data.active_sessions[sessionId];
    data.ended_games[sessionId] = {
        endedAt: new Date().toISOString(),
        reason,
        gameType
    };
}

function removePlayerFromActiveSession(data, sessionId, playerId, reason) {
    const activeSession = data.active_sessions[sessionId];
    if (!activeSession) {
        delete data.active_games[playerId];
        return null;
    }

    const index = activeSession.players.findIndex((player) => player.playerId === playerId);
    if (index === -1) {
        delete data.active_games[playerId];
        return activeSession;
    }

    activeSession.players.splice(index, 1);
    activeSession.replayReadyPlayerIds = activeSession.replayReadyPlayerIds.filter((entry) => entry !== playerId);
    if (!activeSession.replayDeclinedPlayerIds.includes(playerId)) {
        activeSession.replayDeclinedPlayerIds.push(playerId);
    }
    activeSession.updatedAt = new Date().toISOString();
    delete data.active_games[playerId];

    io.to(playerId).emit('session-cleared', { playerId, sessionId, reason });

    if (activeSession.players.length === 0) {
        delete data.active_sessions[sessionId];
        data.ended_games[sessionId] = {
            endedAt: new Date().toISOString(),
            reason,
            gameType: activeSession.gameType
        };
        return null;
    }

    syncActiveGamesForSession(data, activeSession);
    return activeSession;
}

function maybeTerminateReplayIfUnavailable(data, activeSession, reason = 'replay_unavailable') {
    if (!activeSession || activeSession.gameType !== 'openbox') {
        return false;
    }
    if (!['roundEnded', 'replayWaiting'].includes(activeSession.status)) {
        return false;
    }

    const connectedEligibleCount = activeSession.players.filter((player) => isPlayerConnected(player.playerId)).length;
    if (activeSession.players.length >= OPENBOX_MIN_REPLAY_PLAYERS && connectedEligibleCount >= OPENBOX_MIN_REPLAY_PLAYERS) {
        return false;
    }

    clearActiveSession(data, activeSession.sessionId, reason, 'replay-unavailable');
    return true;
}

async function createSessionForMatch(gameType, options, candidates) {
    const configCheck = validateGameServerConfig(gameType);
    if (!configCheck.ok) {
        return { ok: false, message: configCheck.message };
    }

    const playerIds = candidates.map((entry) => entry.playerId);
    const playerCount = options.playerCount ?? options.mode;
    const authToken = gameType === 'openbox' ? OPENBOX_CONTROL_TOKEN : DLQ_PASSWORD;

    const url =
        gameType === 'dice'
            ? DICE_GAME_SERVER_URL
            : gameType === 'tictactoe'
                ? TICTACTOE_GAME_SERVER_URL
                : OPENBOX_GAME_SERVER_URL;

    const startPath = gameType === 'openbox' ? '/session/start' : '/start';
    const body =
        gameType === 'dice'
            ? { playerCount: options.mode, turnTimeMs: DICE_TURN_TIME_MS }
            : gameType === 'tictactoe'
                ? { turnDurationSec: TICTACTOE_TURN_DURATION_SEC }
                : { playerCount, stakeAmount: options.stakeAmount, playerIds };

    for (let attempt = 1; attempt <= MAX_SESSION_CREATION_ATTEMPTS; attempt += 1) {
        try {
            console.log(`[Game Server] Attempt ${attempt} to create ${gameType} session`, body);
            const response = await fetch(`${url}${startPath}`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'Authorization': `Bearer ${authToken}`
                },
                body: JSON.stringify(body)
            });

            const receivedSignature = response.headers.get('x-hub-signature-256');
            const rawBody = await response.text();
            if (!response.ok) {
                throw new Error(`Game server returned status ${response.status}: ${rawBody}`);
            }
            if (!receivedSignature) {
                throw new Error('Response from game server is missing signature header');
            }

            const computedSignature = crypto.createHmac('sha256', HMAC_SECRET).update(rawBody).digest('hex');
            if (!crypto.timingSafeEqual(Buffer.from(receivedSignature), Buffer.from(computedSignature))) {
                throw new Error('Invalid response signature from game server');
            }

            const parsed = JSON.parse(rawBody);
            if (!parsed.sessionId || !parsed.joinUrl) {
                throw new Error('Invalid response payload from game server');
            }

            return { ok: true, sessionId: parsed.sessionId, joinUrl: parsed.joinUrl };
        } catch (error) {
            console.error(`[Error] Attempt ${attempt} failed:`, error.message);
            if (attempt < MAX_SESSION_CREATION_ATTEMPTS) {
                await delay(SESSION_CREATION_RETRY_DELAY_MS);
            } else {
                return { ok: false, message: error.message };
            }
        }
    }

    return { ok: false, message: 'Unknown error creating session' };
}

async function requestOpenBoxReplay(sessionId, playerIds) {
    const configCheck = validateGameServerConfig('openbox');
    if (!configCheck.ok) {
        throw new Error(configCheck.message);
    }

    const response = await fetch(`${OPENBOX_GAME_SERVER_URL}/session/${encodeURIComponent(sessionId)}/replay`, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${OPENBOX_CONTROL_TOKEN}`
        },
        body: JSON.stringify({ playerIds })
    });

    const rawBody = await response.text();
    if (!response.ok) {
        throw new Error(`Open Box replay failed with status ${response.status}: ${rawBody}`);
    }

    try {
        return JSON.parse(rawBody);
    } catch {
        return {};
    }
}

async function attemptMatchmaking(gameType, options) {
    const matchingKey = getMatchmakingKey(gameType, options);
    if (matchingLocks.has(matchingKey)) return;
    matchingLocks.add(matchingKey);

    try {
        await db.read();
        ensureQueueStructure(db.data);

        let queue = getQueueBucket(db.data, gameType, options);
        const requiredPlayers = options.requiredPlayers;

        while (queue.length >= requiredPlayers) {
            const candidates = [];
            while (queue.length > 0 && candidates.length < requiredPlayers) {
                const entry = queue.shift();
                const socket = io.sockets.sockets.get(entry.socketId);
                if (!socket || socket.disconnected) {
                    console.log(`[State] Dropping queued player ${entry.playerId} (socket offline).`);
                    continue;
                }
                candidates.push(entry);
            }

            if (candidates.length < requiredPlayers) {
                queue.unshift(...candidates);
                break;
            }

            await db.write();
            const sessionResult = await createSessionForMatch(gameType, options, candidates);

            await db.read();
            ensureQueueStructure(db.data);
            queue = getQueueBucket(db.data, gameType, options);

            if (!sessionResult.ok) {
                console.error(`[Fatal] Failed to create ${gameType} session: ${sessionResult.message}`);
                for (const entry of candidates.reverse()) {
                    getQueueBucket(db.data, gameType, options).unshift(entry);
                    io.to(entry.playerId).emit('match-error', {
                        message: `Could not create game session. ${sessionResult.message}`
                    });
                }
                await db.write();
                break;
            }

            const activeSession = buildActiveSessionRecord(gameType, options, sessionResult, candidates);
            syncActiveGamesForSession(db.data, activeSession);
            await db.write();

            for (const player of activeSession.players) {
                io.to(player.playerId).emit('match-found', buildMatchFoundPayload(db.data.active_games[player.playerId]));
            }
        }

        broadcastQueueStatus();
    } finally {
        matchingLocks.delete(matchingKey);
    }
}

function handleOpenBoxLifecycleEvent(data, eventName, payload) {
    const sessionId = payload?.sessionId;
    if (!sessionId) {
        return { handled: false, reason: 'missing_session_id' };
    }

    const activeSession = data.active_sessions[sessionId];
    if (!activeSession) {
        return { handled: false, reason: 'unknown_session' };
    }

    activeSession.lastEventType = eventName;
    activeSession.updatedAt = new Date().toISOString();

    if (eventName === 'round.ended') {
        activeSession.status = 'roundEnded';
        activeSession.roundId = payload.roundId || activeSession.roundId || null;
        activeSession.roundNumber = payload.roundNumber || activeSession.roundNumber || null;
        syncActiveGamesForSession(data, activeSession);
        emitToPlayers(activeSession.players.map((player) => player.playerId), 'replay-status', buildReplayStatusPayload(activeSession));
        return { handled: true };
    }

    if (eventName === 'session.replay_waiting') {
        activeSession.status = 'replayWaiting';
        activeSession.roundId = payload.roundId || activeSession.roundId || null;
        activeSession.roundNumber = payload.roundNumber || activeSession.roundNumber || null;
        activeSession.replayReadyPlayerIds = [];
        activeSession.replayDeclinedPlayerIds = [];
        syncActiveGamesForSession(data, activeSession);
        emitToPlayers(activeSession.players.map((player) => player.playerId), 'replay-status', buildReplayStatusPayload(activeSession));
        return { handled: true };
    }

    if (eventName === 'session.replay_started') {
        activeSession.status = 'active';
        activeSession.roundId = payload.roundId || activeSession.roundId || null;
        activeSession.roundNumber = payload.roundNumber || activeSession.roundNumber || null;
        activeSession.replayStartedAt = new Date().toISOString();
        activeSession.replayReadyPlayerIds = [];
        activeSession.replayDeclinedPlayerIds = [];
        syncActiveGamesForSession(data, activeSession);
        emitToPlayers(activeSession.players.map((player) => player.playerId), 'replay-started', {
            sessionId: activeSession.sessionId,
            roundId: activeSession.roundId,
            roundNumber: activeSession.roundNumber,
            playerIds: activeSession.players.map((player) => player.playerId)
        });
        return { handled: true };
    }

    if (eventName === 'round.cancelled') {
        clearActiveSession(data, sessionId, payload.endReason || 'round_cancelled');
        return { handled: true };
    }

    if (eventName === 'session.ended') {
        clearActiveSession(data, sessionId, payload.endReason || 'session_ended');
        return { handled: true };
    }

    return { handled: false, reason: 'ignored_event' };
}

async function main() {
    await initializeDatabase();

    app.post('/session-closed', verifyWebhookSignature, async (req, res) => {
        const payload = req.body || {};
        const sessionId = payload.sessionId;
        const eventName = payload.eventName || 'session.ended';

        console.log(`[Webhook] Received session-closed callback for session ${sessionId || 'unknown'} (${eventName})`);

        if (!sessionId) {
            return res.status(400).send('Bad Request: sessionId is required.');
        }

        try {
            await db.read();
            ensureQueueStructure(db.data);

            const activeSession = db.data.active_sessions[sessionId];
            if (!activeSession && !getSessionPlayerIds(db.data, sessionId).length) {
                console.log(`[Webhook] No active players found for session ${sessionId}. It might have already been cleared.`);
                return res.status(200).send('Session already cleared or unknown.');
            }

            clearActiveSession(db.data, sessionId, payload.endReason || payload.reason || 'session_ended');
            await db.write();
            broadcastQueueStatus();

            res.status(200).send('Session successfully closed.');
        } catch (error) {
            console.error(`[FATAL] Error processing /session-closed for session ${sessionId}:`, error);
            res.status(500).send('Internal Server Error.');
        }
    });

    app.post('/webhooks/open-box', verifyWebhookSignature, async (req, res) => {
        const payload = req.body || {};
        const eventName = req.headers['x-event-type'] || payload.eventName || '';
        if (!eventName) {
            return res.status(400).send('Missing event type.');
        }

        try {
            await db.read();
            ensureQueueStructure(db.data);
            const result = handleOpenBoxLifecycleEvent(db.data, String(eventName), payload);
            await db.write();
            broadcastQueueStatus();
            res.status(200).json({ ok: true, handled: result.handled, reason: result.reason || null });
        } catch (error) {
            console.error('[FATAL] Error processing Open Box lifecycle webhook:', error);
            res.status(500).send('Internal Server Error.');
        }
    });

    app.get('/health', (req, res) => {
        res.json({
            ok: true,
            queueStatus: buildQueueStatus(db.data || {}),
            openBox: {
                configured: !!OPENBOX_GAME_SERVER_URL,
                clientConfigured: !!normalizeBaseUrl(OPENBOX_CLIENT_URL)
            },
            dbEntryTtlMs: DB_ENTRY_TTL_MS
        });
    });

    io.on('connection', (socket) => {
        console.log(`[Socket] Client connected: ${socket.id}`);

        db.read()
            .then(() => {
                ensureQueueStructure(db.data);
                socket.emit('queue-status', buildQueueStatus(db.data));
            })
            .catch((err) => {
                console.error('[Socket] Failed to send initial queue status:', err.message);
            });

        socket.on('request-match', async (data) => {
            try {
                const { playerId, playerName, gameType: rawGameType } = data || {};
                if (!playerId || !playerName) {
                    return socket.emit('match-error', { message: 'playerId and playerName are required.' });
                }

                const gameType = normalizeGameType(rawGameType);
                if (!gameType) {
                    return socket.emit('match-error', { message: 'Invalid gameType. Use dice, tictactoe, or openbox.' });
                }

                const options = resolveRequestOptions(gameType, data || {});
                if (options.error) {
                    return socket.emit('match-error', { message: options.error });
                }

                socket.join(playerId);
                console.log(`[Socket] Match requested by PlayerID: ${playerId} (${gameType})`, options);

                await db.read();
                ensureQueueStructure(db.data);

                const activeGame = db.data.active_games[playerId];
                if (activeGame) {
                    const activeSession = db.data.active_sessions[activeGame.sessionId] || null;
                    if (db.data.ended_games[activeGame.sessionId]) {
                        delete db.data.active_games[playerId];
                    } else {
                        socket.emit('match-found', buildMatchFoundPayload(activeGame));
                        if (activeSession?.gameType === 'openbox' && ['roundEnded', 'replayWaiting'].includes(activeSession.status)) {
                            socket.emit('replay-status', buildReplayStatusPayload(activeSession));
                        }
                        await db.write();
                        return;
                    }
                }

                const rateCheck = registerQueueAction(db.data, playerId, { blockOnLimit: true });
                if (rateCheck.blocked) {
                    await db.write();
                    return socket.emit('match-error', {
                        message: 'Cooldown active. Please wait before re-queueing.',
                        cooldownUntil: rateCheck.cooldownUntil
                    });
                }

                removeFromQueues(db.data, playerId);

                const queueBucket = getQueueBucket(db.data, gameType, options);
                const existing = queueBucket.find((entry) => entry.playerId === playerId);
                if (existing) {
                    existing.playerName = playerName;
                    existing.socketId = socket.id;
                    existing.queuedAt = new Date().toISOString();
                    existing.gameType = gameType;
                    existing.mode = options.mode ?? null;
                    existing.playerCount = options.playerCount ?? null;
                    existing.stakeAmount = options.stakeAmount ?? null;
                } else {
                    queueBucket.push({
                        playerId,
                        playerName,
                        socketId: socket.id,
                        gameType,
                        mode: options.mode ?? null,
                        playerCount: options.playerCount ?? null,
                        stakeAmount: options.stakeAmount ?? null,
                        queuedAt: new Date().toISOString()
                    });
                }

                await db.write();
                broadcastQueueStatus();
                attemptMatchmaking(gameType, options);
            } catch (err) {
                console.error('[FATAL] Unhandled error in request-match handler:', err);
                socket.emit('match-error', { message: 'An unexpected server error occurred.' });
            }
        });

        socket.on('cancel-match', async (data) => {
            try {
                const { playerId } = data || {};
                if (!playerId) {
                    return socket.emit('match-error', { message: 'playerId is required to cancel.' });
                }

                await db.read();
                ensureQueueStructure(db.data);
                const removed = removeFromQueues(db.data, playerId);
                registerQueueAction(db.data, playerId, { blockOnLimit: false });
                await db.write();

                if (removed) {
                    socket.emit('queue-cancelled', { playerId });
                }
                broadcastQueueStatus();
            } catch (err) {
                console.error('[FATAL] Unhandled error in cancel-match handler:', err);
                socket.emit('match-error', { message: 'An unexpected server error occurred while cancelling.' });
            }
        });

        socket.on('request-replay', async (data) => {
            try {
                const { playerId } = data || {};
                if (!playerId) {
                    return socket.emit('match-error', { message: 'playerId is required to request replay.' });
                }

                await db.read();
                ensureQueueStructure(db.data);
                const activeGame = db.data.active_games[playerId];
                if (!activeGame || activeGame.gameType !== 'openbox') {
                    return socket.emit('match-error', { message: 'No active Open Box session found for replay.' });
                }

                const activeSession = db.data.active_sessions[activeGame.sessionId];
                if (!activeSession || !['roundEnded', 'replayWaiting'].includes(activeSession.status)) {
                    return socket.emit('match-error', { message: 'Replay is not currently available.' });
                }

                if (!activeSession.players.some((player) => player.playerId === playerId)) {
                    return socket.emit('match-error', { message: 'Player is not eligible for replay.' });
                }

                if (!activeSession.replayReadyPlayerIds.includes(playerId)) {
                    activeSession.replayReadyPlayerIds.push(playerId);
                }
                syncActiveGamesForSession(db.data, activeSession);

                socket.emit('replay-requested', {
                    sessionId: activeSession.sessionId,
                    playerId
                });

                emitToPlayers(activeSession.players.map((player) => player.playerId), 'replay-status', buildReplayStatusPayload(activeSession));

                if (activeSession.replayReadyPlayerIds.length >= OPENBOX_MIN_REPLAY_PLAYERS) {
                    const replayPlayerIds = [...activeSession.replayReadyPlayerIds];
                    const replayResult = await requestOpenBoxReplay(activeSession.sessionId, replayPlayerIds);

                    activeSession.status = 'active';
                    activeSession.roundId = replayResult.roundId || activeSession.roundId || null;
                    activeSession.roundNumber = replayResult.roundNumber || activeSession.roundNumber || null;
                    activeSession.replayStartedAt = new Date().toISOString();
                    activeSession.replayReadyPlayerIds = [];
                    activeSession.replayDeclinedPlayerIds = [];
                    syncActiveGamesForSession(db.data, activeSession);

                    await db.write();
                    emitToPlayers(activeSession.players.map((player) => player.playerId), 'replay-started', {
                        sessionId: activeSession.sessionId,
                        roundId: activeSession.roundId,
                        roundNumber: activeSession.roundNumber,
                        playerIds: activeSession.players.map((player) => player.playerId)
                    });
                    return;
                }

                await db.write();
            } catch (err) {
                console.error('[FATAL] Unhandled error in request-replay handler:', err);
                socket.emit('match-error', { message: 'Replay request failed.' });
            }
        });

        socket.on('exit-session', async (data) => {
            try {
                const { playerId } = data || {};
                if (!playerId) {
                    return socket.emit('match-error', { message: 'playerId is required to exit.' });
                }

                await db.read();
                ensureQueueStructure(db.data);
                const activeGame = db.data.active_games[playerId];
                if (!activeGame) {
                    await db.write();
                    return socket.emit('session-cleared', { playerId, sessionId: null, reason: 'no_active_session' });
                }

                const activeSession = removePlayerFromActiveSession(db.data, activeGame.sessionId, playerId, 'player_exited');
                if (activeSession && activeSession.gameType === 'openbox') {
                    if (!maybeTerminateReplayIfUnavailable(db.data, activeSession, 'replay_unavailable')) {
                        emitToPlayers(activeSession.players.map((player) => player.playerId), 'replay-status', buildReplayStatusPayload(activeSession));
                    }
                }

                await db.write();
                broadcastQueueStatus();
            } catch (err) {
                console.error('[FATAL] Unhandled error in exit-session handler:', err);
                socket.emit('match-error', { message: 'An unexpected server error occurred while exiting.' });
            }
        });

        socket.on('disconnect', async () => {
            console.log(`[Socket] Client disconnected: ${socket.id}`);
            try {
                await db.read();
                ensureQueueStructure(db.data);

                let removedPlayer = null;
                for (const bucket of getQueueBuckets(db.data)) {
                    const index = bucket.findIndex((entry) => entry.socketId === socket.id);
                    if (index !== -1) {
                        removedPlayer = bucket.splice(index, 1)[0];
                        break;
                    }
                }

                if (removedPlayer) {
                    console.log(`[State] Player ${removedPlayer.playerId} removed from queue due to disconnect.`);
                }

                for (const session of Object.values(db.data.active_sessions)) {
                    if (session.gameType !== 'openbox' || !['roundEnded', 'replayWaiting'].includes(session.status)) {
                        continue;
                    }
                    if (maybeTerminateReplayIfUnavailable(db.data, session, 'replay_unavailable')) {
                        break;
                    }
                    emitToPlayers(session.players.map((player) => player.playerId), 'replay-status', buildReplayStatusPayload(session));
                }

                await db.write();
                broadcastQueueStatus();
            } catch (err) {
                console.error('[FATAL] Unhandled error in disconnect handler:', err);
            }
        });

        socket.on('report-invalid-session', async (data) => {
            try {
                const { playerId, sessionId } = data || {};
                if (!playerId || !sessionId) {
                    return socket.emit('match-error', { message: 'playerId and sessionId are required to report an invalid session.' });
                }

                await db.read();
                ensureQueueStructure(db.data);
                const activeGame = db.data.active_games[playerId];

                if (activeGame && activeGame.sessionId === sessionId) {
                    console.log(`[State] Player ${playerId} reported invalid session ${sessionId}. Clearing active game entry.`);
                    const activeSession = removePlayerFromActiveSession(db.data, sessionId, playerId, 'invalid_session_reported');
                    if (activeSession && activeSession.gameType === 'openbox') {
                        if (!maybeTerminateReplayIfUnavailable(db.data, activeSession, 'replay_unavailable')) {
                            emitToPlayers(activeSession.players.map((player) => player.playerId), 'replay-status', buildReplayStatusPayload(activeSession));
                        }
                    }
                    await db.write();
                    broadcastQueueStatus();
                } else {
                    console.warn(
                        `[State] Player ${playerId} sent an invalid report for session ${sessionId}. Their active session is ${activeGame ? activeGame.sessionId : 'non-existent'}.`
                    );
                    socket.emit('match-error', { message: 'Invalid session report. You are not in that session.' });
                }
            } catch (err) {
                console.error('[FATAL] Unhandled error in report-invalid-session handler:', err);
                socket.emit('match-error', { message: 'An unexpected server error occurred while reporting session.' });
            }
        });
    });

    server.listen(PORT, () => {
        console.log(`Matchmaking server listening on http://localhost:${PORT}`);
    });
}

main();
