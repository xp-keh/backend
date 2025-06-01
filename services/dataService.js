require("dotenv").config();
const moment = require("moment-timezone");
const pool = require("../config/postgis");
const clickhouse = require("../config/clickhouse");

const SEISMIC_DB = process.env.SEISMIC_DB;
const WEATHER_DB = process.env.WEATHER_DB;

async function fetchData(start_time, end_time, longitude, latitude, radius = 25000) {
  const formattedStart = moment.utc(start_time).format("YYYY-MM-DD");
  const formattedEnd = moment.utc(end_time).format("YYYY-MM-DD");

  console.log("[INFO] Fetching relevant table pairs from PostGIS...");
  const { rows: catalogRows } = await pool.query(`
    SELECT table_name, data_type
    FROM data_catalog
    WHERE date BETWEEN $1 AND $2
      AND ST_DWithin(geom, ST_SetSRID(ST_MakePoint($3, $4), 4326)::geography, $5)
  `, [formattedStart, formattedEnd, longitude, latitude, radius]);

  const weatherTables = catalogRows.filter(r => r.data_type === 'weather').map(r => r.table_name);
  const seismicTables = catalogRows.filter(r => r.data_type === 'seismic').map(r => r.table_name);

  let joinedRows = [];

  for (const weatherTable of weatherTables) {
    const stationCode = weatherTable.split("_")[1];
    const datePart = weatherTable.split("_")[2];

    const seismicTable = seismicTables.find(t => t.includes(stationCode) && t.includes(datePart));
    if (!seismicTable) continue;

    console.log(`[INFO] Joining ${weatherTable} with ${seismicTable}...`);

    console.log(`Start time:`, start_time)
    console.log(`End time:`, end_time)

    const joinQuery = `
      SELECT *
      FROM
      (
          SELECT
              parseDateTimeBestEffort(timestamp) AS parsed_timestamp,
              station,
              anyIf(data, channel = 'BHE') AS BHE,
              anyIf(data, channel = 'BHN') AS BHN,
              anyIf(data, channel = 'BHZ') AS BHZ
          FROM ${SEISMIC_DB}.${seismicTable}
          WHERE parseDateTimeBestEffort(timestamp) BETWEEN toDateTime('${start_time}') AND toDateTime('${end_time}')
          GROUP BY parsed_timestamp, station
      ) AS s
      ASOF INNER JOIN
      (
          SELECT
              temp AS temprature,
              humidity,
              wind_speed,
              wind_deg AS wind_degree,
              location,
              parseDateTimeBestEffort(timestamp) AS parsed_timestamp
          FROM ${WEATHER_DB}.${weatherTable}
          WHERE parseDateTimeBestEffort(timestamp) BETWEEN toDateTime('${start_time}') AND toDateTime('${end_time}')
      ) AS w
      ON s.station = w.location AND s.parsed_timestamp >= w.parsed_timestamp
`;


    const result = await clickhouse.query({ query: joinQuery, format: "JSON" });
    const data = (await result.json()).data;
    joinedRows.push(...data);
  }

  console.log(`[INFO] fetchJoinedData finished. Returning ${joinedRows.length} rows.`);
  return { joinedData: joinedRows };
}

module.exports = {
  fetchData
};