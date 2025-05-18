require("dotenv").config();
const moment = require("moment-timezone");
const clickhouse = require("../config/clickhouse");

const SEISMIC_DB = process.env.SEISMIC_DB;
const WEATHER_DB = process.env.WEATHER_DB;

async function fetchData(start_time, end_time, longitude, latitude, radius = 25000, limit, seismicDbName = SEISMIC_DB, weatherDbName = WEATHER_DB) {
  console.log("[INFO] fetchData called with params:", { start_time, end_time, longitude, latitude, radius, limit });

  const formattedStartTime = start_time.replace(' ', 'T');
  const formattedEndTime = end_time.replace(' ', 'T');
  const searchRadius = radius ? parseFloat(radius) : 25000;

  const seismicStartTable = `seismic_${moment.tz(formattedStartTime, "UTC").format("YYYYMMDD_HH")}`;
  const seismicEndTable = `seismic_${moment.tz(formattedEndTime, "UTC").format("YYYYMMDD_HH")}`;
  const weatherStartTable = `weather_${moment.tz(formattedStartTime, "UTC").format("YYYYMMDD_HH")}`;
  const weatherEndTable = `weather_${moment.tz(formattedEndTime, "UTC").format("YYYYMMDD_HH")}`;

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

  let allWeatherData = [];
  for (const weatherTable of weatherTables) {
    const weatherData = await fetchWeatherData(weatherDbName, weatherTable, longitude, latitude, searchRadius);
    allWeatherData.push(...weatherData);
  }

  for (const seismicTable of seismicTables) {
    const seismicData = await fetchSeismicData(seismicDbName, seismicTable, longitude, latitude, searchRadius);

    for (const seismic of seismicData) {
      const seismicMoment = moment.utc(seismic.dt);

      const weather = allWeatherData.reduce((closest, w) => {
        const weatherMoment = moment.utc(w.dt_format, 'DD-MM-YYYYTHH:mm:ss');
        const diff = Math.abs(seismicMoment.diff(weatherMoment, 'seconds'));
        if (diff <= 900 && (!closest || diff < Math.abs(seismicMoment.diff(moment.utc(closest.dt_format, 'DD-MM-YYYYTHH:mm:ss'), 'seconds')))) {
          return w;
        }
        return closest;
      }, null);

      finalData.push({
        Timestamp: seismic.dt,
        Lat: seismic.lat,
        Lon: seismic.lon,
        Network: seismic.network,
        Station: seismic.station,
        BHE: seismic.BHE,
        BHN: seismic.BHN,
        BHZ: seismic.BHZ,
        Temperature: weather?.temp ?? null,
        Humidity: weather?.humidity ?? null,
        Pressure: weather?.pressure ?? null,
        Wind: weather?.wind_speed ?? null,
        Clouds: weather?.clouds ?? null
      });

      if (limit && finalData.length >= limit) break;
    }

    if (limit && finalData.length >= limit) break;
  }

  console.log(`[INFO] fetchData finished. Total joined rows: ${finalData.length}`);
  return { finalData };
}

async function fetchSeismicData(seismicDbName, seismicTable, longitude, latitude, searchRadius) {
  const query = `
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
    WHERE
      greatCircleDistance(lat, lon, ${latitude}, ${longitude}) < ${searchRadius}
    GROUP BY 
      dt,
      lat,
      lon,
      network,
      station
    HAVING countDistinct(channel) = 3
    ORDER BY dt ASC
  `;

  const result = await clickhouse.query({ query, format: "JSON" });
  const data = await result.json();

  const rawSeismic = data?.data || [];

  console.log("[Seismic Data First Row]:", JSON.stringify(rawSeismic[0], null, 2));

  return rawSeismic;
};

async function fetchWeatherData(weatherDbName, weatherTable, longitude, latitude, searchRadius) {
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

module.exports = {
  fetchData
};
