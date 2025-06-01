require("dotenv").config();
const moment = require("moment-timezone");
const pool = require("../config/postgis");
const clickhouse = require("../config/clickhouse");

const SEISMIC_DB = process.env.SEISMIC_DB;
const WEATHER_DB = process.env.WEATHER_DB;

async function fetchPreviewData(start_time, end_time, longitude, latitude, radius = 25000, seismicDbName = SEISMIC_DB, weatherDbName = WEATHER_DB) {
  const previewLimit = 10;
  const formattedStart = moment.utc(start_time).format('YYYY-MM-DD');
  const formattedEnd = moment.utc(end_time).format('YYYY-MM-DD');

  console.log("[INFO] Fetching relevant tables from PostGIS within spatial and temporal bounds...");

  const { rows: catalogRows } = await pool.query(`
    SELECT table_name, data_type
    FROM data_catalog
    WHERE date BETWEEN $1 AND $2
      AND ST_DWithin(geom, ST_SetSRID(ST_MakePoint($3, $4), 4326)::geography, $5)
  `, [formattedStart, formattedEnd, longitude, latitude, radius]);

  const seismicTables = catalogRows.filter(r => r.data_type === 'seismic').map(r => r.table_name);
  const weatherTables = catalogRows.filter(r => r.data_type === 'weather').map(r => r.table_name);

  console.log(`[INFO] Found ${seismicTables.length} seismic tables, ${weatherTables.length} weather tables`);

  const allWeatherData = [];
  for (const weatherTable of weatherTables) {
    if (allWeatherData.length >= previewLimit) break;

    const weatherQuery = `
      SELECT 
        dt_format,
        lat, lon, location, temp, feels_like, pressure, humidity, wind_speed, wind_deg, wind_gust, clouds
      FROM ${weatherDbName}.${weatherTable}
      ORDER BY dt_format ASC
      LIMIT ${previewLimit - allWeatherData.length}
    `;
    const weatherResult = await clickhouse.query({ query: weatherQuery, format: "JSON" });
    const weatherData = (await weatherResult.json()).data;

    allWeatherData.push(...weatherData);
    if (allWeatherData.length >= previewLimit) break;
  }

  let previewData = [];

  for (const weather of allWeatherData) {
    const weatherTime = moment.utc(weather.dt_format, 'DD-MM-YYYYTHH:mm:ss');

    let matchedSeismic = null;
    for (const seismicTable of seismicTables) {
      const seismicQuery = `
        SELECT 
          toDateTime64(dt, 6) AS dt,
          lat,
          lon,
          network,
          station,
          maxIf(data, channel = 'BHE') AS BHE,
          maxIf(data, channel = 'BHN') AS BHN,
          maxIf(data, channel = 'BHZ') AS BHZ
        FROM ${seismicDbName}.${seismicTable}
        WHERE dt >= '${weatherTime.clone().subtract(15, 'minutes').format("YYYY-MM-DD HH:mm:ss")}'
          AND dt <= '${weatherTime.clone().add(15, 'minutes').format("YYYY-MM-DD HH:mm:ss")}'
        GROUP BY dt, lat, lon, network, station
        HAVING countDistinct(channel) = 3
        ORDER BY dt ASC
        LIMIT 1
      `;
      const seismicResult = await clickhouse.query({ query: seismicQuery, format: "JSON" });
      const seismicData = (await seismicResult.json()).data;
      if (seismicData.length > 0) {
        matchedSeismic = seismicData[0];
        break;
      }
    }

    previewData.push({
      Timestamp: matchedSeismic?.dt ?? null,
      Lat: matchedSeismic?.lat ?? null,
      Lon: matchedSeismic?.lon ?? null,
      Network: matchedSeismic?.network ?? null,
      Station: matchedSeismic?.station ?? null,
      BHE: matchedSeismic?.BHE ?? null,
      BHN: matchedSeismic?.BHN ?? null,
      BHZ: matchedSeismic?.BHZ ?? null,
      Temperature: weather.temp ?? null,
      Humidity: weather.humidity ?? null,
      Pressure: weather.pressure ?? null,
      Wind: weather.wind_speed ?? null,
      Clouds: weather.clouds ?? null
    });
  }

  console.log(`[INFO] fetchPreviewData completed. Returning ${previewData.length} preview rows.`);
  return { previewData };
}

module.exports = {
  fetchPreviewData,
};
