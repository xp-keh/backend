require("dotenv").config();

const moment = require("moment-timezone");
const pool = require("../config/postgis");
const clickhouse = require("../config/clickhouse");

const SEISMIC_DB = process.env.SEISMIC_DB;
const WEATHER_DB = process.env.WEATHER_DB;
const SEARCH_RADIUS = 25000;

async function relevantTables(data_type, start_time, end_time, latitude, longitude, radius) {
  const formattedStart = moment.utc(start_time).format("YYYY-MM-DD");
  const formattedEnd = moment.utc(end_time).format("YYYY-MM-DD");

  const { rows } = await pool.query(`
    SELECT table_name 
    FROM data_catalog 
    WHERE data_type = $1 
      AND date BETWEEN $2 AND $3
      AND ST_DWithin(
        geom,
        ST_SetSRID(ST_MakePoint($4, $5), 4326)::geography,
        $6
      )
    ORDER BY table_name ASC;
  `, [data_type, formattedStart, formattedEnd, longitude, latitude, radius]);

  return rows.map(r => r.table_name);
}

async function getSeismicGraphData({ start_time, end_time, latitude, longitude }) {
  const tableNames = await relevantTables('seismic', start_time, end_time, latitude, longitude, SEARCH_RADIUS);

  const startUnixTime = moment.utc(start_time).unix();
  const endUnixTime = moment.utc(end_time).unix();

  const thirtyMinutesLater = startUnixTime + 1800;
  const adjustedEndTime = Math.min(endUnixTime, thirtyMinutesLater);

  const queryTasks = tableNames.map(async table => {
    const query = `
      SELECT 
        dt_format, timestamp, lat, lon, network, station,
        maxIf(data, channel = 'BHE') AS BHE,
        maxIf(data, channel = 'BHN') AS BHN,
        maxIf(data, channel = 'BHZ') AS BHZ
      FROM ${SEISMIC_DB}.${table}
      WHERE 
        dt_format BETWEEN ${startUnixTime} AND ${adjustedEndTime}
      GROUP BY dt_format, timestamp, lat, lon, network, station
      HAVING countDistinct(channel) = 3
      ORDER BY timestamp ASC;
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
    const ts = item.timestamp;
    if (item.BHZ !== undefined) graphData.hnz.push({ timestamp: ts, value: item.BHZ });
    if (item.BHN !== undefined) graphData.hnn.push({ timestamp: ts, value: item.BHN });
    if (item.BHE !== undefined) graphData.hne.push({ timestamp: ts, value: item.BHE });
  });

  return graphData;
}

async function getWeatherGraphData({ start_time, end_time, latitude, longitude }) {
  const tableNames = await relevantTables('weather', start_time, end_time, latitude, longitude, SEARCH_RADIUS);

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
        dt BETWEEN toDateTime('${start_time}') AND toDateTime('${end_time}')
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
