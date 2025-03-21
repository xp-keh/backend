require('dotenv').config();

var createError = require('http-errors');
var express = require('express');
var path = require('path');
var cookieParser = require('cookie-parser');
var logger = require('morgan');
var cors = require('cors');

var authRouter = require('./routes/auth');
var indexRouter = require('./routes/index');
var catalogRouter = require('./routes/catalog');
var retrieveRouter = require('./routes/retrieve');
const helmet = require('helmet');
const xss = require('xss-clean');

var app = express();

app.use(helmet());
app.use(xss());

// ==== CORS Setup ====
// Allow credentials (cookies), specific origins, and headers
app.use(cors({
  origin: process.env.CLIENT_URL || 'http://localhost:3000', // replace with your frontend URL
  credentials: true, // allow cookies to be sent
  methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Authorization'],
}));

// view engine setup
app.set('views', path.join(__dirname, 'views'));
app.set('view engine', 'jade');

app.use(logger('dev'));
app.use(express.json());
app.use(express.urlencoded({ extended: false }));
app.use(cookieParser());
app.use(express.static(path.join(__dirname, 'public')));

app.use('/', indexRouter);
app.use('/auth', authRouter);
app.use('/catalog', catalogRouter);
app.use('/retrieve', retrieveRouter);

// catch 404 and forward to error handler
app.use(function(req, res, next) {
  next(createError(404));
});

// error handler
app.use(function(err, req, res, next) {
  // set locals, only providing error in development
  res.locals.message = err.message;
  res.locals.error = req.app.get('env') === 'development' ? err : {};

  // render the error page
  res.status(err.status || 500);
  res.render('error');
});

module.exports = app;
