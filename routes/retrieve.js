const express = require("express");
const router = express.Router();
const moment = require("moment-timezone");
const pool = require("../config/postgis");
const clickhouse = require("../config/clickhouse");
const { runAgent } = require('../config/ragAgent');
const { Parser } = require("json2csv");
const fs = require("fs");
const path = require("path");

const { fetchPreviewData } = require("../services/previewService");
const { fetchData } = require("../services/dataService");
const { getSeismicGraphData, getWeatherGraphData } = require("../services/graphService");

router.get("/cities", async (req, res) => {
  try {
    const cityQuery = `
      SELECT city, longitude, latitude
      FROM cities
      ORDER BY city ASC;
    `;

    const { rows: cities } = await pool.query(cityQuery);

    res.json({ cities });
  } catch (error) {
    console.error("Error fetching cities:", error);
    res.status(500).json({ error: "Internal Server Error" });
  }
});

router.get("/preview", async (req, res) => {
  try {
    const { start_time, end_time, longitude, latitude, radius } = req.query;
    const { previewData } = await fetchPreviewData(start_time, end_time, longitude, latitude, radius);

    console.log("[INFO] PreviewData to return:", previewData.length, "rows");

    res.json({ previewData });
  } catch (err) {
    console.error("Error fetching preview data:", err);
    res.status(500).json({ error: "Failed to fetch preview data" });
  }
});

router.get("/download", async (req, res) => {
  try {
    const { start_time, end_time, longitude, latitude, radius, city } = req.query;
    const { joinedData } = await fetchData(start_time, end_time, longitude, latitude, radius);

    if (joinedData.length === 0) return res.status(404).json({ error: "No data found" });

    const json2csvParser = new Parser();
    const csv = json2csvParser.parse(joinedData);
    const downloadsDir = path.join(__dirname, "../public/downloads");

    if (!fs.existsSync(downloadsDir)) fs.mkdirSync(downloadsDir, { recursive: true });

    const cityLabel = (city || "location").toLowerCase().replace(/\s+/g, "_");
    const startDateStr = moment.utc(start_time).format("YYYYMMDD_HHmm");
    const endDateStr = moment.utc(end_time).format("YYYYMMDD_HHmm");
    const filename = `seismic_weather_${cityLabel}_${startDateStr}_to_${endDateStr}.csv`;

    const filePath = path.join(downloadsDir, filename);
    fs.writeFileSync(filePath, csv);

    res.download(filePath, filename, (err) => {
      if (err) console.error("Error sending file:", err);
      fs.unlinkSync(filePath);
    });
  } catch (error) {
    console.error("Error fetching data:", error);
    res.status(500).json({ error: "Internal Server Error" });
  }
});

router.get("/seismic-graph", async (req, res) => {
  try {
    const graphData = await getSeismicGraphData(req.query);
    res.json({ graph_data: graphData });
  } catch (err) {
    console.error("Error building seismic graph:", err);
    res.status(500).json({ error: "Internal Server Error" });
  }
});

router.get("/weather-graph", async (req, res) => {
  try {
    const graphData = await getWeatherGraphData(req.query);
    res.json({ graph_data: graphData });
  } catch (err) {
    console.error("Error building weather graph:", err);
    res.status(500).json({ error: "Internal Server Error" });
  }
});

function buildRAGQuestion({ lon, lat, start_date, end_date }) {
  const sameDay = start_date === end_date;

  if (sameDay) {
    return `What was the weather like on ${start_date} at location (${lat}, ${lon})?`;
  } else {
    return `What was the weather like between ${start_date} and ${end_date} at location (${lat}, ${lon})?`;
  }
}

router.post('/summary', async (req, res) => {
  try {
    const { lon, lat, start_date, end_date } = req.body;

    const question = buildRAGQuestion({ lon, lat, start_date, end_date });

    const result = await runAgent({ question, lat, lon, start_date, end_date });

    if (result.status === "error") {
      return res.status(400).json(result);
    }

    return res.json(result);
  } catch (error) {
    console.error("[SERVER ERROR]", error);
    return res.status(500).json({
      status: "error",
      code: "SERVER_ERROR",
      message: "Something went wrong on our side.",
    });
  }
});

module.exports = router;
