var express = require('express');
var router = express.Router();

const authenticateToken = require('../middleware/authMiddleware');

router.get('/health', (req, res) => {
  res.status(200).json({ status: 'ok' });
});

module.exports = router;
