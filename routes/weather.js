require("dotenv").config();
const express = require("express");
const router = express.Router();
const { createClient } = require("@clickhouse/client");
const moment = require("moment-timezone");
const axios = require("axios");

const clickhouse = createClient({
  url: process.env.CLICKHOUSE_URL,
  username: process.env.CLICKHOUSE_USER,
  password: process.env.CLICKHOUSE_PASSWORD,
  database: process.env.CLICKHOUSE_DB_WEATHER,
});

const API_KEY = "7794c2f0e827d159325b614c8b7945a5";
const BASE_URL_h = "https://pro.openweathermap.org/data/2.5/forecast/hourly";
const BASE_URL_d = "https://pro.openweathermap.org/data/2.5/forecast/daily";

const cityCoordinates = {
  Kretek: { lat: "-7.9923", lon: "110.2973" },
  Jogjakarta: { lat: "-7.8021", lon: "110.3628" },
  Menggoran: { lat: "-7.9525", lon: "110.4942" },
  Bandara_DIY: { lat: "-7.9007", lon: "110.0573" },
  Bantul: { lat: "-7.8750", lon: "110.3268" },
};

async function fetchWeatherData(type, city) {
  try {
    const now = moment().tz("UTC");
    const twoDaysAgo = now.clone().subtract(1, "days");

    const startTable = `weather_${twoDaysAgo.format("YYYYMMDD_HHmm")}`;
    const endTable = `weather_${now.format("YYYYMMDD_HHmm")}`;

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

    let unionQuery;
    if (type === "temp") {
      unionQuery = `
        SELECT location, dt, 
            argMax(temp, dt) AS temp
        FROM (
            ${tableNames
              .map(
                (table) => `
                    SELECT location, temp, dt
                    FROM weather_dev_1.${table}
                `
              )
              .join(" UNION ALL ")}
        )
        GROUP BY location, dt
        ORDER BY dt ASC;
      `;
    } else if (type === "humidity") {
      unionQuery = `
        SELECT location, dt, 
            argMax(humidity, dt) AS humidity
        FROM (
            ${tableNames
              .map(
                (table) => `
                    SELECT location, humidity, dt
                    FROM weather_dev_1.${table}
                `
              )
              .join(" UNION ALL ")}
        )
        GROUP BY location, dt
        ORDER BY dt ASC;
      `;
    } else if (type === "wind") {
      unionQuery = `
        SELECT location, dt, 
            argMax(wind_speed, dt) AS wind_speed, 
            argMax(wind_deg, dt) AS wind_deg,
            argMax(wind_gust, dt) AS wind_gust
        FROM (
            ${tableNames
              .map(
                (table) => `
                    SELECT location, wind_speed, wind_deg, wind_gust, dt
                    FROM weather_dev_1.${table}
                `
              )
              .join(" UNION ALL ")}
        )
        GROUP BY location, dt
        ORDER BY dt ASC;
      `;
    } else {
      throw new Error("Invalid data type specified");
    }

    const weatherDataResult = await clickhouse.query({
      query: unionQuery,
      format: "JSON",
    });
    const weatherData = await weatherDataResult.json();

    return weatherData.data;
  } catch (error) {
    console.error(`Error fetching weather data for ${type}:`, error);
    throw error;
  }
}

router.get("/forecast_next_5_hours", async (req, res) => {
  try {
    const forecasts = await Promise.all(
      Object.entries(cityCoordinates).map(async ([city, { lat, lon }]) => {
        const response = await axios.get(BASE_URL_h, {
          params: { lat, lon, appid: API_KEY, units: "metric" },
        });

        return response.data.list.slice(0, 5).map((entry) => ({
          location: city,
          dt: moment
            .unix(entry.dt)
            .tz("Asia/Jakarta")
            .format("YYYY-MM-DD HH:mm:ss"),
          temp: entry.main.temp,
          description: entry.weather[0].description,
        }));
      })
    );

    res.json({ forecast: forecasts.flat() });
  } catch (error) {
    console.error("Error fetching forecast:", error);
    res.status(500).json({ error: "Failed to fetch forecast data" });
  }
});

router.get("/forecast_next_7_days", async (req, res) => {
  try {
    const forecasts = await Promise.all(
      Object.entries(cityCoordinates).map(async ([city, { lat, lon }]) => {
        const response = await axios.get(BASE_URL_d, {
          params: { lat, lon, appid: API_KEY, units: "metric", cnt: 8 },
        });

        return response.data.list.slice(1, 7).map((entry) => ({
          location: city,
          dt: moment.unix(entry.dt).tz("Asia/Jakarta").format("YYYY-MM-DD"),
          temp: entry.temp.day,
          description: entry.weather[0].description,
        }));
      })
    );

    res.json({ forecast: forecasts.flat() });
  } catch (error) {
    console.error("Error fetching 8-day forecast:", error);
    res.status(500).json({ error: "Failed to fetch forecast data" });
  }
});

router.get("/fetch_weather", async (req, res) => {
  const type = req.query.type;
  try {
    const data = await fetchWeatherData(type);
    res.json({ data: data });
  } catch (error) {
    res.status(500).json({ error: "Failed to fetch temperature data" });
  }
});

module.exports = router;
