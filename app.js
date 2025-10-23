require("dotenv").config();

var createError = require("http-errors");
var express = require("express");
var path = require("path");
var cookieParser = require("cookie-parser");
var logger = require("morgan");
var cors = require("cors");
const helmet = require("helmet");
const xss = require("xss-clean");

var authRouter = require("./routes/auth");
var indexRouter = require("./routes/index");
var catalogRouter = require("./routes/catalog");
var retrieveRouter = require("./routes/retrieve");
var weatherRouter = require("./routes/weather");
var seismicRouter = require("./routes/seismic");
// Temporarily disabled for local development - use unified file explorer instead
// var weatherMinioRouter = require("./routes/weatherMinio");
// var seismicMinioRouter = require("./routes/seismicMinio");
// var readerMinioRouter = require("./routes/readerMinio");
var fileExplorerRouter = require("./routes/fileExplorer");
var app = express();

app.use(helmet());
app.use(xss());

app.use(
  cors({
    origin: ["http://localhost:3000", "http://85.209.163.202:3000"],
    credentials: true,
    methods: ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allowedHeaders: ["Content-Type", "Authorization"],
  })
);

app.set("views", path.join(__dirname, "views"));
app.set("view engine", "jade");

app.use(logger("dev"));
app.use(express.json());
app.use(express.urlencoded({ extended: false }));
app.use(cookieParser());
app.use(express.static(path.join(__dirname, "public")));

app.use("/", indexRouter);
app.use("/auth", authRouter);
app.use("/catalog", catalogRouter);
app.use("/retrieve", retrieveRouter);
app.use("/weather", weatherRouter);
app.use("/seismic", seismicRouter);
// Temporarily disabled for local development - use unified file explorer instead
// app.use("/weather-minio", weatherMinioRouter);
// app.use("/seismic-minio", seismicMinioRouter);
// app.use("/reader-minio", readerMinioRouter);
app.use("/files", fileExplorerRouter);

app.use(function (req, res, next) {
  next(createError(404));
});

app.use(function (err, req, res, next) {
  res.locals.message = err.message;
  res.locals.error = req.app.get("env") === "development" ? err : {};

  res.status(err.status || 500);
  res.render("error", {
    title: "Error",
    message: err.message,
    error: err,
  });
});

module.exports = app;
