const jwt = require('jsonwebtoken');
require('dotenv').config();

const authenticateToken = (req, res, next) => {
    const authHeader = req.headers['authorization'];
    if (!authHeader) return res.status(401).json({ error: 'Access Denied. No token provided.' });

    const token = authHeader.split(' ')[1];
    if (!token) return res.status(401).json({ error: 'Access Denied. Invalid token format.' });

    jwt.verify(token, process.env.JWT_SECRET, (err, user) => {
        if (err) return res.status(403).json({ error: 'Invalid or expired token' });

        req.user = user;
        next();
    });
};

module.exports = authenticateToken;