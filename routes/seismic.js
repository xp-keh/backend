require("dotenv").config();
const express = require("express");
const router = express.Router();
const { createClient } = require("@clickhouse/client");
const moment = require("moment-timezone");
// const axios = require("axios");

const clickhouse = createClient({
  url: process.env.CLICKHOUSE_URL,
  username: process.env.CLICKHOUSE_USER,
  password: process.env.CLICKHOUSE_PASSWORD,
  database: process.env.CLICKHOUSE_DB_SEISMIC,
});

async function fetchSeismicData(station, type) {
  try {
    const now = moment().tz("UTC");
    const lastday = now.clone().subtract(10, "minutes");

    const startTable = `seismic_${lastday.format("YYYYMMDD_HHmm")}`;
    const endTable = `seismic_${now.format("YYYYMMDD_HHmm")}`;

    const tableListQuery = `
            SELECT name 
            FROM system.tables 
            WHERE database = 'seismic_dev_1' 
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
      console.log("No tables found for the last 24 hour.");
      return [];
    }

    let unionQuery;
    unionQuery = `
        SELECT dt, station, data
        FROM (
            ${tableNames
              .map(
                (table) => `
                    SELECT dt, station, data
                    FROM seismic_dev_1.${table}
                    WHERE channel='${type}' AND station='${station}'

                `
              )
              .join(" UNION ALL ")}
        )
        ORDER BY dt ASC;
      `;

    const seismicDataResult = await clickhouse.query({
      query: unionQuery,
      format: "JSON",
    });
    const seismicData = await seismicDataResult.json();

    return seismicData.data;
  } catch (error) {
    console.error(`Error fetching weather data for ${type}:`, error);
    throw error;
  }
}

router.get("/fetch_seismic", async (req, res) => {
  const station = req.query.station;
  const axis = req.query.axis;
  try {
    const data = await fetchSeismicData(station, axis);
    res.json({ bhz: data });
  } catch (error) {
    res.status(500).json({ error: "Failed to fetch seismic Z data" });
  }
});

module.exports = router;
