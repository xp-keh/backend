const express = require("express");
const router = express.Router();
const moment = require("moment-timezone");
const pool = require("../config/postgis");
const clickhouse = require("../config/clickhouse");
const { Parser } = require("json2csv");
const fs = require("fs");
const path = require("path");

async function fetchData(start_time, end_time, longitude, latitude, radius, limit) {
  const formattedStartTime = moment.tz(start_time, "Asia/Jakarta").utc().format();
  const formattedEndTime = moment.tz(end_time, "Asia/Jakarta").utc().format();
  const searchRadius = radius ? parseFloat(radius) : 25000;

  const catalogQuery = `
    SELECT source_type, spatial_extent, partition_date, table_name,
           ST_X(ST_Centroid(spatial_extent::geometry)) AS x,
           ST_Y(ST_Centroid(spatial_extent::geometry)) AS y
    FROM data_catalog
    WHERE partition_date BETWEEN $1 AND $2
    AND ST_DWithin(
        spatial_extent::geography, 
        ST_SetSRID(ST_MakePoint($3, $4), 4326)::geography, 
        $5
    );
  `;

  const { rows: catalogRows } = await pool.query(catalogQuery, [
    formattedStartTime, formattedEndTime, longitude, latitude, searchRadius,
  ]);

  let partitionMap = {};
  for (const row of catalogRows) {
    const { source_type, table_name, partition_date, x, y } = row;
    if (!partitionMap[partition_date]) {
      partitionMap[partition_date] = [];
    }
    partitionMap[partition_date].push({ source_type, table_name, x, y });
  }

  let finalData = [];
  let partitionedResults = [];
  for (const partitionDate in partitionMap) {
    const sources = partitionMap[partitionDate];
    for (let i = 0; i < sources.length; i++) {
      for (let j = i + 1; j < sources.length; j++) {
        if (sources[i].source_type !== sources[j].source_type) {
          const seismicTable = sources[i].source_type === "seismic" ? sources[i].table_name : sources[j].table_name;
          const weatherTable = sources[i].source_type === "weather" ? sources[i].table_name : sources[j].table_name;

          const clickhouseQuery = `
          WITH seismic_data AS (
              SELECT timestamp AS seismic_timestamp, elevation, longitude AS seismic_longitude, latitude AS seismic_latitude,
                  station_code, hnz, hnn, hne
              FROM seismic.${seismicTable}
              WHERE timestamp BETWEEN '${start_time}' AND '${end_time}'
          ),
          weather_data AS (
              SELECT timestamp AS weather_timestamp, longitude AS weather_longitude, latitude AS weather_latitude,
                  sys_country, clouds_all, wind_speed, wind_deg, main_pressure, main_sea_level, 
                  main_grnd_level, main_temp, main_temp_min, main_temp_max, main_feels_like, main_humidity,
                  visibility, weather_main, weather_description, weather_icon
              FROM weather.${weatherTable}
              WHERE timestamp BETWEEN '${start_time}' AND '${end_time}'
          )
          SELECT s.*, w.*, greatCircleDistance(s.seismic_latitude, s.seismic_longitude, w.weather_latitude, w.weather_longitude) AS distance
          FROM seismic_data AS s
          LEFT JOIN weather_data AS w
          ON s.seismic_timestamp = w.weather_timestamp
          WHERE distance < ${searchRadius}
          ORDER BY s.seismic_timestamp ${limit ? `LIMIT ${limit}` : ""};
      `;

          try {
            const result = await clickhouse.query({ query: clickhouseQuery, format: "JSON" });
            const data = await result.json();
            if (data && data.data) {
              finalData = finalData.concat(data.data);
              partitionedResults.push({
                partition_date: partitionDate,
                seismic_table: seismicTable,
                weather_table: weatherTable,
                data: data.data,
              });
            }
          } catch (err) {
            console.error("Error executing ClickHouse query:", err);
          }
        }
      }
    }
  }
  return { finalData, partitionedResults };
}

router.get("/preview", async (req, res) => {
  try {
    const { partitionedResults } = await fetchData(
      req.query.start_time,
      req.query.end_time,
      req.query.longitude,
      req.query.latitude,
      req.query.radius,
      10
    );

    const merged_data = partitionedResults.flatMap(({ partition_date, seismic_table, weather_table, data }) =>
      data.map(item => ({
        ...item,
        partition_date,
        seismic_table,
        weather_table,
      }))
    );

    res.json({ merged_data });
  } catch (error) {
    console.error("Error fetching data:", error);
    res.status(500).json({ error: "Internal Server Error" });
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

router.get("/seismic-graph", async (req, res) => {
  try {
    const { start_time, end_time, longitude, latitude, radius } = req.query;

    console.log(`[INFO] /seismic-graph called with params:`, {
      start_time, end_time, longitude, latitude, radius
    });

    const formattedStartTime = moment.tz(start_time, "Asia/Jakarta").utc().format();
    const formattedEndTime = moment.tz(end_time, "Asia/Jakarta").utc().format();
    const searchRadius = radius ? parseFloat(radius) : 25000;

    const catalogQuery = `
      SELECT table_name
      FROM data_catalog
      WHERE source_type = 'seismic'
        AND partition_date BETWEEN $1 AND $2
        AND ST_DWithin(
          spatial_extent::geography,
          ST_SetSRID(ST_MakePoint($3, $4), 4326)::geography,
          $5
        );
    `;

    const { rows: catalogRows } = await pool.query(catalogQuery, [
      formattedStartTime, formattedEndTime, longitude, latitude, searchRadius,
    ]);

    console.log(`[INFO] Found ${catalogRows.length} matching seismic tables`);

    const queryTasks = catalogRows.map(async ({ table_name }) => {
      const clickhouseQuery = `
        SELECT 
          toStartOfInterval(timestamp, INTERVAL 5 second) AS interval_time,
          avg(hnz) AS hnz_avg,
          avg(hnn) AS hnn_avg,
          avg(hne) AS hne_avg
        FROM seismic.${table_name}
        WHERE timestamp BETWEEN '${start_time}' AND '${end_time}'
        GROUP BY interval_time
        ORDER BY interval_time;
      `;

      try {
        const result = await clickhouse.query({ query: clickhouseQuery, format: "JSON" });
        const data = await result.json();
        return data?.data || [];
      } catch (err) {
        console.error(`[ERROR] Failed query for ${table_name}:`, err);
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


module.exports = router;
