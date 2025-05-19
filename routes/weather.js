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
  TNTI: { lat: "0.7718", lon: "127.3667" },
  TOLI: { lat: "1.1214", lon: "120.7944" },
  GENI: { lat: "-2.5927", lon: "140.1678" },
  PMBI: { lat: "-2.9024", lon: "104.6993" },
  BKB: { lat: "-1.1073", lon: "116.9048" },
  SOEI: { lat: "-9.7553", lon: "124.2672" },
  SANI: { lat: "-2.0496", lon: "125.9881" },
  MMRI: { lat: "-8.6357", lon: "122.2376" },
  PMBT: { lat: "-2.9270", lon: "104.7720" },
  TOLI2: { lat: "1.11119", lon: "120.78174" },
  BKNI: { lat: "0.3262", lon: "101.0396" },
  UGM: { lat: "-7.9125", lon: "110.5231" },
  FAKI: { lat: "-2.91925", lon: "132.24889" },
  CISI: { lat: "-7.5557", lon: "107.8153" },
  BNDI: { lat: "-4.5224", lon: "129.9045" },
  PLAI: { lat: "-8.8275", lon: "117.7765" },
  MNAI: { lat: "-4.3605", lon: "102.9557" },
  GSI: { lat: "1.3039", lon: "97.5755" },
  SMRI: { lat: "-7.04915", lon: "110.44067" },
  SAUI: { lat: "-7.9826", lon: "131.2988" },
  YOGI: { lat: "-7.8166", lon: "110.2949" },
  LHMI: { lat: "5.2288", lon: "96.9472" },
  LUWI: { lat: "-1.0418", lon: "122.7717" },
  JAGI: { lat: "-8.4702", lon: "114.1521" },
};

async function fetchWeatherData(type, city) {
  try {
    const now = moment().tz("UTC");
    const endTime = now.clone().startOf("hour");
    const startTime = endTime.clone().subtract(24, "hours");
    const yesterday = now.clone().subtract(1, "days");
    const startTable = `weather_${city}_${yesterday.format("YYYYMMDD")}`;
    const endTable = `weather_${city}_${now.format("YYYYMMDD")}`;

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
    console.log(tableNames);

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
                    WHERE dt >= ${startTime.unix()}
                `
              )
              .join(" UNION ALL ")}
        )
        GROUP BY location, dt
        ORDER BY dt ASC;
      `;
    } else if (type === "hum") {
      unionQuery = `
        SELECT location, dt, 
            argMax(humidity, dt) AS humidity
        FROM (
            ${tableNames
              .map(
                (table) => `
                    SELECT location, humidity, dt
                    FROM weather_dev_1.${table}
                    WHERE dt >= ${startTime.unix()}
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
                    WHERE dt >= ${startTime.unix()}
                `
              )
              .join(" UNION ALL ")}
        )
        GROUP BY location, dt
        ORDER BY dt ASC;
      `;
      //   `
      //   SELECT location, wind_speed, wind_deg, wind_gust, dt
      //   FROM (
      //       ${tableNames
      //         .map(
      //           (table) => `
      //             SELECT location, wind_speed, wind_deg, wind_gust, dt
      //             FROM weather_dev_1.${table}
      //             WHERE location = '${city}'
      //           `
      //         )
      //         .join(" UNION ALL ")}
      //   )
      //   ORDER BY dt ASC;
      // `;
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
  const city = req.query.city;
  try {
    const data = await fetchWeatherData(type, city);
    res.json({ data: data });
  } catch (error) {
    res.status(500).json({ error: "Failed to fetch temperature data" });
  }
});

module.exports = router;
