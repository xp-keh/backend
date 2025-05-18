require("dotenv").config();

const moment = require("moment-timezone");
const clickhouse = require("../config/clickhouse");

const SEISMIC_DB = process.env.SEISMIC_DB;
const WEATHER_DB = process.env.WEATHER_DB;
const SEARCH_RADIUS = 25000;


async function listTables(database, startTime, endTime, prefix) {
  const startTable = `${prefix}_${moment.tz(startTime, "UTC").format("YYYYMMDD_HH")}`;
  const endTable = `${prefix}_${moment.tz(endTime, "UTC").format("YYYYMMDD_HH")}`;

  const query = `
    SELECT name 
    FROM system.tables 
    WHERE database = '${database}' 
      AND name >= '${startTable}' 
      AND name <= '${endTable}'
    ORDER BY name ASC;
  `;

  const result = await clickhouse.query({ query, format: "JSON" });
  const json = await result.json();
  return json.data.map(row => row.name);
}

async function getSeismicGraphData({ start_time, end_time, latitude, longitude }) {
  const tableNames = await listTables(SEISMIC_DB, start_time, end_time, "seismic");

  const queryTasks = tableNames.map(async table => {
    const query = `
      SELECT 
        toStartOfInterval(toDateTime64(dt, 6), toIntervalMillisecond(10000)) AS dt,
        lat, lon, network, station,
        maxIf(data, channel = 'BHE') AS BHE,
        maxIf(data, channel = 'BHN') AS BHN,
        maxIf(data, channel = 'BHZ') AS BHZ
      FROM ${SEISMIC_DB}.${table}
      WHERE 
        greatCircleDistance(lat, lon, ${latitude}, ${longitude}) < ${SEARCH_RADIUS}
        AND dt BETWEEN toDateTime('${start_time}') AND toDateTime('${end_time}')
      GROUP BY dt, lat, lon, network, station
      HAVING countDistinct(channel) = 3
      ORDER BY dt ASC;
    `;
    try {
      const result = await clickhouse.query({ query, format: "JSON" });
      const json = await result.json();
      return json.data || [];
    } catch (err) {
      console.error(`[ERROR] Query failed for table ${table}:`, err.message);
      return [];
    }
  });

  const results = await Promise.all(queryTasks);
  const allData = results.flat();

  const graphData = { hnz: [], hnn: [], hne: [] };
  allData.forEach(item => {
    const ts = item.dt;
    if (item.BHZ !== undefined) graphData.hnz.push({ timestamp: ts, value: item.BHZ });
    if (item.BHN !== undefined) graphData.hnn.push({ timestamp: ts, value: item.BHN });
    if (item.BHE !== undefined) graphData.hne.push({ timestamp: ts, value: item.BHE });
  });

  return graphData;
}

async function getWeatherGraphData({ start_time, end_time, latitude, longitude }) {
  const tableNames = await listTables(WEATHER_DB, start_time, end_time, "weather");

  const queryTasks = tableNames.map(async table => {
    const query = `
      SELECT 
        toStartOfInterval(toDateTime64(dt, 3), toIntervalMillisecond(10000)) AS dt_format,
        avg(temp) AS avg_temp,
        avg(feels_like) AS avg_feels_like,
        avg(pressure) AS avg_pressure,
        avg(humidity) AS avg_humidity,
        avg(wind_speed) AS avg_wind_speed,
        avg(wind_deg) AS avg_wind_deg,
        avg(wind_gust) AS avg_wind_gust,
        avg(clouds) AS avg_clouds
      FROM ${WEATHER_DB}.${table}
      WHERE 
        greatCircleDistance(lat, lon, ${latitude}, ${longitude}) < ${SEARCH_RADIUS}
        AND dt BETWEEN toDateTime('${start_time}') AND toDateTime('${end_time}')
      GROUP BY dt_format
      ORDER BY dt_format ASC;
    `;
    try {
      const result = await clickhouse.query({ query, format: "JSON" });
      const json = await result.json();
      return json.data || [];
    } catch (err) {
      console.error(`[ERROR] Weather query failed for table ${table}:`, err.message);
      return [];
    }
  });

  const results = await Promise.all(queryTasks);
  const allData = results.flat();

  const graphData = { temp: [], humidity: [], wind: [] };
  allData.forEach(item => {
    const ts = item.dt_format;
    if (item.avg_temp !== undefined) graphData.temp.push({ timestamp: ts, value: item.avg_temp });
    if (item.avg_humidity !== undefined) graphData.humidity.push({ timestamp: ts, value: item.avg_humidity });
    if (item.avg_wind_speed !== undefined) graphData.wind.push({ timestamp: ts, value: item.avg_wind_speed });
  });

  return graphData;
}

module.exports = {
  getSeismicGraphData,
  getWeatherGraphData,
};
