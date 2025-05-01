const express = require("express");
const router = express.Router();
const moment = require("moment-timezone");
const pool = require("../config/postgis");
const clickhouse = require("../config/clickhouse");
const { runAgent } = require('../config/ragAgent');
const { Parser } = require("json2csv");
const fs = require("fs");
const path = require("path");

async function fetchPreviewData(start_time, end_time, longitude, latitude, radius = 25000, seismicDbName = "seismic_dev_3", weatherDbName = "weather_dev_3") {
  console.log("[INFO] fetchPreviewData called with params:", { start_time, end_time, longitude, latitude, radius });

  const formattedStartTime = start_time.replace(' ', 'T');
  const formattedEndTime = end_time.replace(' ', 'T');
  const searchRadius = parseFloat(radius);

  const seismicStartTable = `seismic_${moment.tz(formattedStartTime, "UTC").format("YYYYMMDD_HHmm")}`;
  const seismicEndTable = `seismic_${moment.tz(formattedEndTime, "UTC").format("YYYYMMDD_HHmm")}`;

  const weatherStartTable = `weather_${moment.tz(formattedStartTime, "UTC").format("YYYYMMDD_HHmm")}`;
  const weatherEndTable = `weather_${moment.tz(formattedEndTime, "UTC").format("YYYYMMDD_HHmm")}`;

  const findTablesQuery = (dbName, startTable, endTable) => `
    SELECT name 
    FROM system.tables 
    WHERE database = '${dbName}' 
      AND name >= '${startTable}' 
      AND name <= '${endTable}'
    ORDER BY name ASC;
  `;

  const seismicTablesResult = await clickhouse.query({ query: findTablesQuery(seismicDbName, seismicStartTable, seismicEndTable), format: 'JSON' });
  const weatherTablesResult = await clickhouse.query({ query: findTablesQuery(weatherDbName, weatherStartTable, weatherEndTable), format: 'JSON' });

  const seismicTables = (await seismicTablesResult.json()).data.map(row => row.name);
  const weatherTables = (await weatherTablesResult.json()).data.map(row => row.name);

  let previewData = [];

  for (const seismicTable of seismicTables) {
    const tableTimePart = seismicTable.replace('seismic_', '');
    const matchingWeatherTable = weatherTables.find(wt => wt.endsWith(tableTimePart));
    if (!matchingWeatherTable) continue;

    // Limit seismic query to 50 rows to avoid overload
    const seismicQuery = `
      SELECT 
        formatDateTime(dt, '%d-%m-%YT%H:%i:%S') AS dt,
        lat, lon, network, station, BHE, BHN, BHZ
      FROM ${seismicDbName}.${seismicTable}
      WHERE greatCircleDistance(lat, lon, ${latitude}, ${longitude}) < ${searchRadius}
      ORDER BY dt ASC
      LIMIT 250
    `;

    const weatherQuery = `
      SELECT 
        dt_format,
        lat, lon, location, temp, feels_like, pressure, humidity, wind_speed, wind_deg, wind_gust, clouds
      FROM ${weatherDbName}.${matchingWeatherTable}
      WHERE greatCircleDistance(lat, lon, ${latitude}, ${longitude}) < ${searchRadius}
      ORDER BY dt_format ASC
      LIMIT 250
    `;

    const seismicResult = await clickhouse.query({ query: seismicQuery, format: "JSON" });
    const weatherResult = await clickhouse.query({ query: weatherQuery, format: "JSON" });

    const seismicData = (await seismicResult.json()).data;
    const weatherData = (await weatherResult.json()).data;

    // Join manually
    const seenTimestamps = new Set();

    for (const seismic of seismicData) {
      if (seenTimestamps.has(seismic.dt)) continue;

      const weather = weatherData.find(w => seismic.dt === w.dt_format);

      previewData.push({
        Timestamp: seismic.dt,
        Lat: seismic.lat,
        Lon: seismic.lon,
        Network: seismic.network,
        Station: seismic.station,
        BHE: seismic.BHE,
        BHN: seismic.BHN,
        BHZ: seismic.BHZ,
        Temprature: weather?.temp ?? null,
        Humidity: weather?.humidity ?? null,
        Pressure: weather?.pressure ?? null,
        Wind: weather?.wind_speed ?? null,
        Clouds: weather?.clouds ?? null
      });

      seenTimestamps.add(seismic.dt);

      if (previewData.length >= 10) break;
    }


    if (previewData.length >= 10) break;
  }

  console.log(`[INFO] fetchPreviewData completed. Returning ${previewData.length} preview rows.`);
  return { previewData };
}

const fetchSeismicData = async (seismicDbName, seismicTable, longitude, latitude, searchRadius) => {
  const query = `
    SELECT 
      dt,
      lat,
      lon,
      network,
      station,
      BHE,
      BHN,
      BHZ
    FROM ${seismicDbName}.${seismicTable}
    WHERE 
      greatCircleDistance(lat, lon, ${latitude}, ${longitude}) < ${searchRadius}
    ORDER BY dt DESC
  `;

  const result = await clickhouse.query({ query, format: "JSON" });
  const data = await result.json();

  const rawSeismic = data?.data || [];

  const processedSeismic = rawSeismic
    .map(item => {
      const dt = moment.utc(item.dt); // parse seismic dt as UTC
      if (!dt.isValid()) return null;
      return {
        ...item,
        dt: dt.startOf('second').format("DD-MM-YYYYTHH:mm:ss")
      };
    })
    .filter(Boolean);

  console.log("[Seismic Data First Row]:", JSON.stringify(processedSeismic[0], null, 2));

  return processedSeismic;
};

const fetchWeatherData = async (weatherDbName, weatherTable, longitude, latitude, searchRadius) => {
  const query = `
    SELECT 
      dt_format,
      lat,
      lon,
      location,
      temp,
      feels_like,
      pressure,
      humidity,
      wind_speed,
      wind_deg,
      wind_gust,
      clouds
    FROM ${weatherDbName}.${weatherTable}
    WHERE 
      greatCircleDistance(lat, lon, ${latitude}, ${longitude}) < ${searchRadius}
    ORDER BY dt_format DESC
  `;

  const result = await clickhouse.query({ query, format: "JSON" });
  const data = await result.json();

  const rawWeather = data?.data || [];

  const processedWeather = rawWeather

  console.log("[Weather Data First Row]:", JSON.stringify(processedWeather[0], null, 2));

  return processedWeather;
};

function joinSeismicAndWeatherStrict(seismicData, weatherData) {
  const results = [];

  for (const seismic of seismicData) {
    const matchingWeather = weatherData.find(weather => seismic.dt === weather.dt_format);

    if (matchingWeather) {
      results.push({
        ...seismic,
        ...matchingWeather
      });
    } else {
      results.push({
        ...seismic
      });
    }
  }

  return results;
}

async function fetchData(start_time, end_time, longitude, latitude, radius = 25000, limit, seismicDbName = "seismic_dev_3", weatherDbName = "weather_dev_3") {
  console.log("[INFO] fetchData called with params:", { start_time, end_time, longitude, latitude, radius, limit });

  const formattedStartTime = start_time.replace(' ', 'T');
  const formattedEndTime = end_time.replace(' ', 'T');

  const searchRadius = radius ? parseFloat(radius) : 25000;

  const seismicStartTable = `seismic_${moment.tz(formattedStartTime, "UTC").format("YYYYMMDD_HHmm")}`;
  const seismicEndTable = `seismic_${moment.tz(formattedEndTime, "UTC").format("YYYYMMDD_HHmm")}`;

  const weatherStartTable = `weather_${moment.tz(formattedStartTime, "UTC").format("YYYYMMDD_HHmm")}`;
  const weatherEndTable = `weather_${moment.tz(formattedEndTime, "UTC").format("YYYYMMDD_HHmm")}`;

  const findTablesQuery = (dbName, startTable, endTable) => `
    SELECT name 
    FROM system.tables 
    WHERE database = '${dbName}' 
      AND name >= '${startTable}' 
      AND name <= '${endTable}'
    ORDER BY name ASC;
  `;

  const seismicTablesResult = await clickhouse.query({ query: findTablesQuery(seismicDbName, seismicStartTable, seismicEndTable), format: 'JSON' });
  const weatherTablesResult = await clickhouse.query({ query: findTablesQuery(weatherDbName, weatherStartTable, weatherEndTable), format: 'JSON' });

  const seismicTables = (await seismicTablesResult.json()).data.map(row => row.name);
  const weatherTables = (await weatherTablesResult.json()).data.map(row => row.name);

  console.log("[INFO] Found Seismic Tables:", seismicTables);
  console.log("[INFO] Found Weather Tables:", weatherTables);

  let finalData = [];

  const startUnixSec = Math.floor(new Date(start_time).getTime() / 1000);
  const endUnixSec = Math.floor(new Date(end_time).getTime() / 1000);


  for (const seismicTable of seismicTables) {
    const tableTimePart = seismicTable.replace('seismic_', '');
    const matchingWeatherTable = weatherTables.find(wt => wt.endsWith(tableTimePart));

    if (!matchingWeatherTable) {
      console.log(`[WARN] No matching weather table for seismic table: ${seismicTable}`);
      continue;
    }

    console.log(`[INFO] Processing Seismic Table: ${seismicTable} and Weather Table: ${matchingWeatherTable}`);

    const seismicData = await fetchSeismicData(seismicDbName, seismicTable, longitude, latitude, searchRadius);
    const weatherData = await fetchWeatherData(weatherDbName, matchingWeatherTable, longitude, latitude, searchRadius);

    const joined = joinSeismicAndWeatherStrict(seismicData, weatherData, startUnixSec, endUnixSec);

    console.log("[Joined Data - First 3 Rows]:", JSON.stringify(joined.slice(0, 3), null, 2));

    finalData = finalData.concat(joined);
  }

  if (limit) {
    finalData = finalData.slice(0, limit);
  }

  console.log(`[INFO] fetchData finished. Total joined rows: ${finalData.length}`);
  return { finalData };
}

router.get("/preview", async (req, res) => {
  try {
    const { start_time, end_time, longitude, latitude, radius } = req.query;
    const { previewData } = await fetchPreviewData(start_time, end_time, longitude, latitude, radius);

    console.log("[INFO] PreviewData to return:", previewData.length, "rows");
    console.dir(previewData.slice(0, 3), { depth: null });

    res.json({ previewData });
  } catch (err) {
    console.error("Error fetching preview data:", err);
    res.status(500).json({ error: "Failed to fetch preview data" });
  }
});

router.get("/download", async (req, res) => {
  try {
    const { finalData } = await fetchData(req.query.start_time, req.query.end_time, req.query.longitude, req.query.latitude, req.query.radius, null);
    if (finalData.length === 0) {
      return res.status(404).json({ error: "No data found" });
    }

    const json2csvParser = new Parser();
    const csv = json2csvParser.parse(finalData);
    const downloadsDir = path.join(__dirname, "../public/downloads");

    // Ensure the downloads directory exists
    if (!fs.existsSync(downloadsDir)) {
      fs.mkdirSync(downloadsDir, { recursive: true });
    }

    const filePath = path.join(downloadsDir, "merged_data.csv");

    fs.writeFileSync(filePath, csv);
    res.download(filePath, "merged_data.csv", (err) => {
      if (err) console.error("Error sending file:", err);
      fs.unlinkSync(filePath);
    });
  } catch (error) {
    console.error("Error fetching data:", error);
    res.status(500).json({ error: "Internal Server Error" });
  }
});

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

function chooseInterval(start, end) {
  const diffMs = new Date(end) - new Date(start);
  const diffMins = diffMs / (1000 * 60);
  if (diffMins <= 60) return "5 second";
  if (diffMins <= 1440) return "1 minute";         // 1 day
  if (diffMins <= 10080) return "5 minute";        // 1 week
  return "15 minute";                              // more than a week
}

router.get("/seismic-graph", async (req, res) => {
  try {
    const { start_time, end_time } = req.query;

    console.log(`[INFO] /seismic-graph called with params:`, {
      start_time, end_time
    });

    const interval = chooseInterval(start_time, end_time);

    const startDate = moment(start_time).utc().format("YYYYMMDD");
    const endDate = moment(end_time).utc().format("YYYYMMDD");

    const listTablesQuery = (dbName, startTable, endTable) => `
      SELECT name 
      FROM system.tables 
      WHERE database = '${dbName}' 
        AND name >= '${startTable}' 
        AND name <= '${endTable}'
      ORDER BY name ASC;
    `;
    const formattedStartTime = start_time.replace(' ', 'T');
    const formattedEndTime = end_time.replace(' ', 'T');

    const seismicStartTable = `seismic_${moment.tz(formattedStartTime, "UTC").format("YYYYMMDD_HHmm")}`;
    const seismicEndTable = `seismic_${moment.tz(formattedEndTime, "UTC").format("YYYYMMDD_HHmm")}`;
    const dbName = "seismic_dev_3"

    const tableResult = await clickhouse.query({ query: listTablesQuery(dbName, seismicStartTable, seismicEndTable), format: "JSON" });
    const tableData = await tableResult.json();

    const tableNames = tableData.data.map(row => row.name);

    console.log(`[INFO] Found ${tableNames.length} seismic tables`);

    const queryTasks = tableNames.map(async (table_name) => {
      const clickhouseQuery = `
        SELECT 
          toStartOfInterval(dt, INTERVAL ${interval}) AS interval_time,
          avg(BHZ) AS hnz_avg,
          avg(BHN) AS hnn_avg,
          avg(BHE) AS hne_avg
        FROM ${dbName}.${table_name}
        WHERE dt BETWEEN toDateTime('${start_time}') AND toDateTime('${end_time}')
        GROUP BY interval_time
        ORDER BY interval_time
        LIMIT 1000;
      `;

      try {
        const result = await clickhouse.query({ query: clickhouseQuery, format: "JSON" });
        const data = await result.json();
        return data?.data || [];
      } catch (err) {
        console.error(`[ERROR] Failed query for ${table_name}:`, err.message);
        return [];
      }
    });

    const allData = await Promise.all(queryTasks);
    const flattened = allData.flat();

    const graphData = { hnz: [], hnn: [], hne: [] };

    flattened.forEach(item => {
      const ts = item.interval_time;
      if (item.hnz_avg !== undefined) graphData.hnz.push({ timestamp: ts, value: item.hnz_avg });
      if (item.hnn_avg !== undefined) graphData.hnn.push({ timestamp: ts, value: item.hnn_avg });
      if (item.hne_avg !== undefined) graphData.hne.push({ timestamp: ts, value: item.hne_avg });
    });

    console.log(`[INFO] Returning graph data with counts:`, {
      hnz: graphData.hnz.length,
      hnn: graphData.hnn.length,
      hne: graphData.hne.length
    });

    res.json({ graph_data: graphData });

  } catch (err) {
    console.error("Error building graph data:", err);
    res.status(500).json({ error: "Internal Server Error" });
  }
});

router.get("/weather-graph", async (req, res) => {
  try {
    const { start_time, end_time } = req.query;

    console.log(`[INFO] /weather-graph called with params:`, {
      start_time, end_time
    });

    const interval = chooseInterval(start_time, end_time);

    // Format table names based on time range
    const formattedStartTime = start_time.replace(' ', 'T');
    const formattedEndTime = end_time.replace(' ', 'T');

    const weatherStartTable = `weather_${moment.tz(formattedStartTime, "UTC").format("YYYYMMDD_HHmm")}`;
    const weatherEndTable = `weather_${moment.tz(formattedEndTime, "UTC").format("YYYYMMDD_HHmm")}`;
    const dbName = "weather_dev_3";

    // Get list of matching tables directly from ClickHouse
    const listTablesQuery = `
      SELECT name 
      FROM system.tables 
      WHERE database = '${dbName}' 
        AND name >= '${weatherStartTable}' 
        AND name <= '${weatherEndTable}'
      ORDER BY name ASC;
    `;

    const tableResult = await clickhouse.query({ query: listTablesQuery, format: "JSON" });
    const tableData = await tableResult.json();
    const tableNames = tableData.data.map(row => row.name);

    console.log(`[INFO] Found ${tableNames.length} weather tables`);

    // Aggregate data from each relevant table
    const queryTasks = tableNames.map(async (table_name) => {
      const clickhouseQuery = `
        SELECT 
          toStartOfInterval(dt, INTERVAL ${interval}) AS interval_time,
          avg(temp) AS avg_temp,
          avg(humidity) AS avg_humidity,
          avg(wind_speed) AS avg_wind_speed
        FROM ${dbName}.${table_name}
        WHERE dt BETWEEN toDateTime('${start_time}') AND toDateTime('${end_time}')
        GROUP BY interval_time
        ORDER BY interval_time
        LIMIT 1000;
      `;

      try {
        const result = await clickhouse.query({ query: clickhouseQuery, format: "JSON" });
        const data = await result.json();
        return data?.data || [];
      } catch (err) {
        console.error(`[ERROR] Failed query for ${table_name}:`, err.message);
        return [];
      }
    });

    const allData = await Promise.all(queryTasks);
    const flattened = allData.flat();

    const graphData = { temp: [], humidity: [], wind: [] };

    flattened.forEach(item => {
      const ts = item.interval_time;
      if (item.avg_temp !== undefined) graphData.temp.push({ timestamp: ts, value: item.avg_temp });
      if (item.avg_humidity !== undefined) graphData.humidity.push({ timestamp: ts, value: item.avg_humidity });
      if (item.avg_wind_speed !== undefined) graphData.wind.push({ timestamp: ts, value: item.avg_wind_speed });
    });

    console.log(`[INFO] Returning weather graph data:`, {
      temp: graphData.temp.length,
      humidity: graphData.humidity.length,
      wind: graphData.wind.length
    });

    res.json({ graph_data: graphData });

  } catch (err) {
    console.error("Error building weather graph data:", err);
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

    // Optional: validate input here (e.g., lat/lon range, date format)

    const question = buildRAGQuestion({ lon, lat, start_date, end_date });

    // Include start_date and end_date in the call to runAgent
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
