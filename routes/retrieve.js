const express = require("express");
const router = express.Router();
const moment = require("moment-timezone");
const pool = require("../config/postgis");
const clickhouse = require("../config/clickhouse");

router.get("/", async (req, res) => {
  try {
    const { start_time, end_time, longitude, latitude, radius } = req.query;

    if (!start_time || !end_time || !longitude || !latitude) {
      return res.status(400).json({ error: "Missing required query parameters" });
    }

    const searchRadius = radius ? parseFloat(radius) : 25000;
    const formattedStartTime = moment.tz(start_time, "Asia/Jakarta").utc().format();
    const formattedEndTime = moment.tz(end_time, "Asia/Jakarta").utc().format();

    console.log("Query Parameters:", req.query);
    console.log("Formatted Start Time (UTC):", formattedStartTime);
    console.log("Formatted End Time (UTC):", formattedEndTime);

    // Step 1: Get Data Catalog from PostGIS
    const catalogQuery = `
      SELECT source_type, spatial_extent, 
             partition_date, table_name,
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

    let mergedData = [];
    let partitionMap = {};

    // Step 2: Process Data Catalog Results
    for (const row of catalogRows) {
      const { source_type, table_name, partition_date, x, y } = row;
      const database = source_type.toLowerCase(); // "seismic" or "weather"

      if (!partitionMap[partition_date]) {
        partitionMap[partition_date] = [];
      }

      partitionMap[partition_date].push({
        source_type,
        table_name,
        x,
        y,
      });
    }

    // Step 3: Fetch Data from ClickHouse
    let finalResults = [];

    for (const partitionDate in partitionMap) {
      const sources = partitionMap[partitionDate];

      for (let i = 0; i < sources.length; i++) {
        for (let j = i + 1; j < sources.length; j++) {
          if (sources[i].source_type !== sources[j].source_type) {
            const seismicTable = sources[i].source_type === "seismic" ? sources[i].table_name : sources[j].table_name;
            const weatherTable = sources[i].source_type === "weather" ? sources[i].table_name : sources[j].table_name;

            const seismicX = sources[i].source_type === "seismic" ? sources[i].x : sources[j].x;
            const seismicY = sources[i].source_type === "seismic" ? sources[i].y : sources[j].y;

            const weatherX = sources[i].source_type === "weather" ? sources[i].x : sources[j].x;
            const weatherY = sources[i].source_type === "weather" ? sources[i].y : sources[j].y;

            const clickhouseQuery = `
            WITH 
                seismic_data AS (
                    SELECT 
                        timestamp AS seismic_timestamp, 
                        elevation, 
                        longitude AS seismic_longitude, 
                        latitude AS seismic_latitude,
                        station_code, 
                        hnz, hnn, hne
                    FROM seismic.${seismicTable}
                    WHERE timestamp BETWEEN '${start_time}' AND '${end_time}'
                ),
                weather_data AS (
                    SELECT 
                        timestamp AS weather_timestamp,
                        longitude AS weather_longitude, 
                        latitude AS weather_latitude,
                        sys_country, 
                        clouds_all, 
                        wind_speed, 
                        wind_deg, 
                        main_pressure, 
                        main_sea_level, 
                        main_grnd_level, 
                        main_temp, 
                        main_temp_min, 
                        main_temp_max, 
                        main_feels_like, 
                        main_humidity, 
                        visibility, 
                        weather_main, 
                        weather_description, 
                        weather_icon
                    FROM weather.${weatherTable}
                    WHERE timestamp BETWEEN '${start_time}' AND '${end_time}'
                )
        
            SELECT 
                s.seismic_timestamp, s.elevation, s.seismic_longitude, s.seismic_latitude,
                s.station_code, s.hnz, s.hnn, s.hne,
                w.weather_timestamp, w.weather_longitude, w.weather_latitude,
                w.sys_country, w.clouds_all, w.wind_speed, w.wind_deg, 
                w.main_pressure, w.main_sea_level, w.main_grnd_level, 
                w.main_temp, w.main_temp_min, w.main_temp_max, 
                w.main_feels_like, w.main_humidity, w.visibility, 
                w.weather_main, w.weather_description, w.weather_icon,
                greatCircleDistance(s.seismic_latitude, s.seismic_longitude, w.weather_latitude, w.weather_longitude) AS distance
            FROM seismic_data AS s
            LEFT JOIN weather_data AS w
            ON s.seismic_timestamp = w.weather_timestamp
            WHERE distance < ${searchRadius}
            ORDER BY s.seismic_timestamp
            LIMIT 50;
        `;

            console.log(`Executing ClickHouse Query: ${clickhouseQuery}`);


            try {
              const result = await clickhouse.query({
                query: clickhouseQuery,
                format: "JSON",
              });

              const data = await result.json();

              if (!data || !data.data) {
                console.error("ClickHouse returned an empty response or incorrect format.");
                return res.status(500).json({ error: "No data returned from ClickHouse." });
              }

              console.log(`Retrieved ${data.data.length} records from ClickHouse.`);
              finalResults.push({
                partition_date: partitionDate,
                seismic_table: seismicTable,
                weather_table: weatherTable,
                data: data.data,
              });

            } catch (err) {
              console.error("Error executing ClickHouse query:", err);
              return res.status(500).json({ error: "Error querying ClickHouse" });
            }
          }
        }
      }
    }

    res.json({ merged_data: finalResults });

  } catch (error) {
    console.error("Error fetching data:", error);
    res.status(500).json({ error: "Internal Server Error" });
  }
});

module.exports = router;
