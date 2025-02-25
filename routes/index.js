var express = require('express');
var router = express.Router();

const authenticateToken = require('../middleware/authMiddleware');

router.get('/', authenticateToken, function (req, res, next) {
  res.send('Hello World!')
});

module.exports = router;
