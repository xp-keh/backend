const { createClient } = require('redis');
require('dotenv').config();

const redis = createClient({
    url: process.env.REDIS_URL
});

redis.on('error', (err) => {
    console.error('[ERROR][Redis]', err.message);
});

const mockRedis = {
    set: async () => console.log('[MOCK][Redis] SET operation (Redis not available)'),
    get: async () => {
        console.log('[MOCK][Redis] GET operation (Redis not available)');
        return null;
    },
    del: async () => console.log('[MOCK][Redis] DEL operation (Redis not available)'),
    exists: async () => {
        console.log('[MOCK][Redis] EXISTS operation (Redis not available)');
        return 0;
    }
};

let connectedRedis = null;

(async () => {
    try {
        await redis.connect();
        connectedRedis = redis;
        console.log('[INFO][Redis] Connected successfully');
    } catch (err) {
        console.log('[INFO][Redis] Using mock Redis client (Redis server not available)');
        connectedRedis = mockRedis;
    }
})();

module.exports = new Proxy({}, {
    get: (target, prop) => {
        if (connectedRedis) {
            return connectedRedis[prop];
        }
        return mockRedis[prop] || (() => Promise.resolve());
    }
});
