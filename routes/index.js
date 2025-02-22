var express = require('express');
var router = express.Router();

const clickhouse = require('../config/clickhouse'); // Import the ClickHouse client
const fs = require('fs');
const fastCsv = require('fast-csv');
const authenticateToken = require('../middleware/authMiddleware');

// Function to get all table names with a specific pattern
const getTableNames = async (pattern) => {
  const query = `SELECT name FROM system.tables WHERE database = 'geospatial' AND name LIKE '${pattern}'`;
  const result = await clickhouse.query({ query, format: 'JSONEachRow' }).then(res => res.json());
  return result.map(row => row.name);
};

// Function to fetch column names
const getColumnNames = async (tableName) => {
  const query = `DESCRIBE TABLE ${tableName}`;
  const result = await clickhouse.query({ query, format: 'JSONEachRow' }).then(res => res.json());
  return result.map(row => row.name);
};

// Function to perform spatial-temporal join and export to CSV
const performJoinAndExport = async (seismicTable, weatherTable) => {
  console.log(`Joining ${seismicTable} with ${weatherTable} based on exact timestamp and nearest coordinates...`);

  // Fetch columns dynamically
  const seismicColumns = await getColumnNames(seismicTable);
  const weatherColumns = await getColumnNames(weatherTable);

  // Ensure both tables have 'timestamp' column
  if (!seismicColumns.includes('timestamp') || !weatherColumns.includes('timestamp')) {
    console.log(`Skipping ${seismicTable} and ${weatherTable}: Missing timestamp column.`);
    return;
  }

  // Construct dynamic SQL query with exact timestamp match
  const query = `
        SELECT ${seismicColumns.map(col => `s.${col}`).join(', ')},
               ${weatherColumns.map(col => `w.${col}`).join(', ')},
               greatCircleDistance(s.latitude, s.longitude, w.latitude, w.longitude) AS distance_km
        FROM ${seismicTable} AS s
        JOIN ${weatherTable} AS w 
        ON s.timestamp = w.timestamp
        WHERE greatCircleDistance(s.latitude, s.longitude, w.latitude, w.longitude) <= 15000
    `;

  const rows = await clickhouse.query({ query, format: 'JSONEachRow' }).then(res => res.json());

  if (!rows.length) {
    console.log(`No matches found for ${seismicTable} and ${weatherTable}. Skipping CSV export.`);
    return;
  }

  // Define CSV filename
  const outputFile = `joined_seismic_weather_${seismicTable}_${weatherTable}.csv`;
  const ws = fs.createWriteStream(outputFile);

  // Write data to CSV
  fastCsv.write(rows, { headers: true }).pipe(ws);

  console.log(`Data exported to ${outputFile}`);
};

// Home Page Route
router.get('/', function (req, res, next) {
  res.send('Hello World!')
});

// Route to trigger the join process
router.get('/join-tables', authenticateToken, async (req, res) => {
  try {
    const seismicTables = await getTableNames('seismic_data_%');
    const weatherTables = await getTableNames('weather_data_%');

    for (const seismicTable of seismicTables) {
      for (const weatherTable of weatherTables) {
        await performJoinAndExport(seismicTable, weatherTable);
      }
    }

    res.json({ message: 'Spatial-temporal joins and exports to CSV completed.' });
  } catch (error) {
    console.error('Error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

module.exports = router;
