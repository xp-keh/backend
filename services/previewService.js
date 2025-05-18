require("dotenv").config();
const moment = require("moment-timezone");
const clickhouse = require("../config/clickhouse");

const SEISMIC_DB = process.env.SEISMIC_DB;
const WEATHER_DB = process.env.WEATHER_DB;

async function fetchPreviewData(start_time, end_time, longitude, latitude, radius = 25000, seismicDbName = SEISMIC_DB, weatherDbName = WEATHER_DB) {
  console.log("[INFO] fetchPreviewData called with params:", { start_time, end_time, longitude, latitude, radius });

  const formattedStartTime = start_time.replace(' ', 'T');
  const formattedEndTime = end_time.replace(' ', 'T');
  const searchRadius = parseFloat(radius);
  const previewLimit = 10;

  const seismicStartTable = `seismic_${moment.tz(formattedStartTime, "UTC").format("YYYYMMDD_HH")}`;
  console.log("[INFO] fetchPreviewData table:", { seismicStartTable });
  const seismicEndTable = `seismic_${moment.tz(formattedEndTime, "UTC").format("YYYYMMDD_HH")}`;
  console.log("[INFO] fetchPreviewData table:", { seismicEndTable });
  const weatherStartTable = `weather_${moment.tz(formattedStartTime, "UTC").format("YYYYMMDD_HH")}`;
  console.log("[INFO] fetchPreviewData table:", { weatherStartTable });
  const weatherEndTable = `weather_${moment.tz(formattedEndTime, "UTC").format("YYYYMMDD_HH")}`;
  console.log("[INFO] fetchPreviewData table:", { weatherEndTable });

  const findTablesQuery = (dbName, startTable, endTable) => `
    SELECT name 
    FROM system.tables 
    WHERE database = '${dbName}' 
      AND name >= '${startTable}' 
      AND name <= '${endTable}'
    ORDER BY name ASC;
  `;

  console.log("[INFO] Query:", { query: findTablesQuery(seismicDbName, seismicStartTable, seismicEndTable) });
  console.log("[INFO] Query:", { query: findTablesQuery(weatherDbName, weatherStartTable, weatherEndTable) });

  const seismicTablesResult = await clickhouse.query({ query: findTablesQuery(seismicDbName, seismicStartTable, seismicEndTable), format: 'JSON' });
  const weatherTablesResult = await clickhouse.query({ query: findTablesQuery(weatherDbName, weatherStartTable, weatherEndTable), format: 'JSON' });

  const seismicTables = (await seismicTablesResult.json()).data.map(row => row.name);
  const weatherTables = (await weatherTablesResult.json()).data.map(row => row.name);

  console.log(`[INFO] fetch tables information complete: ${seismicTables}`);
  console.log(`[INFO] fetch tables information complete: ${weatherTables}`);

  let previewData = [];

  let allWeatherData = [];
  for (const weatherTable of weatherTables) {
    const weatherQuery = `
      SELECT 
        dt_format,
        lat, lon, location, temp, feels_like, pressure, humidity, wind_speed, wind_deg, wind_gust, clouds
      FROM ${weatherDbName}.${weatherTable}
      WHERE greatCircleDistance(lat, lon, ${latitude}, ${longitude}) < ${searchRadius}
      ORDER BY dt_format ASC
    `;
    const weatherResult = await clickhouse.query({ query: weatherQuery, format: "JSON" });
    const weatherData = (await weatherResult.json()).data;
    allWeatherData.push(...weatherData);
  }

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
      WHERE greatCircleDistance(lat, lon, ${latitude}, ${longitude}) < ${searchRadius}
      GROUP BY 
        dt,
        lat,
        lon,
        network,
        station
      HAVING countDistinct(channel) = 3
      ORDER BY dt ASC
    `;

    const seismicResult = await clickhouse.query({ query: seismicQuery, format: "JSON" });
    const seismicData = (await seismicResult.json()).data;

    for (const seismic of seismicData) {
      const seismicMoment = moment.utc(seismic.dt);

      const matchedWeather = allWeatherData.reduce((closest, w) => {
        const weatherMoment = moment.utc(w.dt_format, 'DD-MM-YYYYTHH:mm:ss');
        const diff = Math.abs(seismicMoment.diff(weatherMoment, 'seconds'));
        if (diff <= 900 && (!closest || diff < Math.abs(seismicMoment.diff(moment.utc(closest.dt_format, 'DD-MM-YYYYTHH:mm:ss'), 'seconds')))) {
          return w;
        }
        return closest;
      }, null);

      previewData.push({
        Timestamp: seismic.dt,
        Lat: seismic.lat,
        Lon: seismic.lon,
        Network: seismic.network,
        Station: seismic.station,
        BHE: seismic.BHE,
        BHN: seismic.BHN,
        BHZ: seismic.BHZ,
        Temprature: matchedWeather?.temp ?? null,
        Humidity: matchedWeather?.humidity ?? null,
        Pressure: matchedWeather?.pressure ?? null,
        Wind: matchedWeather?.wind_speed ?? null,
        Clouds: matchedWeather?.clouds ?? null
      });

      if (previewData.length >= previewLimit) break;
    }

    if (previewData.length >= previewLimit) break;
  }

  console.log(`[INFO] fetchPreviewData completed. Returning ${previewData.length} preview rows.`);
  return { previewData };
}

module.exports = {
  fetchPreviewData,
};
