const express = require("express");
const router = express.Router();
// const clickhouse = require("../config/clickhouse");
const { createClient } = require("@clickhouse/client");
const moment = require("moment-timezone");

const clickhouse = createClient({
  url: "http://85.209.163.202:8123",
  username: "abby",
  password: "SpeakLouder",
  database: "weather_dev_1",
});

async function fetchLast2DaysWeather() {
  try {
    const now = moment().tz("UTC");
    const twoDaysAgo = now.clone().subtract(1, "days");

    const startTable = `weather_${twoDaysAgo.format("YYYYMMDD_HHmm")}`;
    const endTable = `weather_${now.format("YYYYMMDD_HHmm")}`;

    console.log(
      `🔍 Searching weather tables from ${startTable} to ${endTable}`
    );

    const tableListQuery = `
            SELECT name 
            FROM system.tables 
            WHERE database = 'weather_dev_1' 
            AND name >= '${startTable}' 
            AND name <= '${endTable}'
            ORDER BY name ASC;
        `;

    const tableListResult = await clickhouse.query({
      query: tableListQuery,
      format: "JSON",
    });

    const tableData = await tableListResult.json();

    const tableNames = tableData.data.map((row) => row.name);

    if (tableNames.length === 0) {
      console.log("No tables found for the last 2 days.");
      return [];
    }

    const unionQuery = `
        SELECT location, dt, 
            argMax(temp, dt) AS temp, 
            argMax(humidity, dt) AS humidity, 
            argMax(wind_speed, dt) AS wind_speed, 
            argMax(wind_deg, dt) AS wind_deg
        FROM (
            ${tableNames
              .map(
                (table) => `
                    SELECT location, temp, humidity, wind_speed, wind_deg, dt 
                    FROM weather_dev_1.${table}
                `
              )
              .join(" UNION ALL ")}
        )
        GROUP BY location, dt
        ORDER BY dt ASC;
    `;

    console.log(`Executing query for tables: ${tableNames.join(", ")}`);

    const weatherDataResult = await clickhouse.query({
      query: unionQuery,
      format: "JSON",
    });
    const weatherData = await weatherDataResult.json();

    return weatherData.data;
  } catch (error) {
    console.error("Error fetching last 2 days' weather data:", error);
    throw error;
  }
}

router.get("/fetch_last_2_days", async (req, res) => {
  try {
    const data = await fetchLast2DaysWeather();
    res.json({ weather_data: data });
  } catch (error) {
    res.status(500).json({ error: "Failed to fetch weather data" });
  }
});

module.exports = router;
