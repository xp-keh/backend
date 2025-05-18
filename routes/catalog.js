const express = require('express');
const router = express.Router();

const clickhouse = require('../config/clickhouse');
const pool = require('../config/postgis');
const moment = require("moment-timezone");

router.post("/register", async (req, res) => {
    const {
        table_name,
        data_type, // 'seismic' or 'weather'
        station_id, // optional for seismic
        date, // e.g. '2025-05-18'
        longitude,
        latitude,
    } = req.body;

    try {
        const query = `
            INSERT INTO data_catalog (
                table_name, data_type, station_id, date, geom, latitude, longitude
            )
            VALUES ($1, $2, $3, $4, ST_SetSRID(ST_MakePoint($5, $6), 4326), $6, $5)
        `;
        await pool.query(query, [table_name, data_type, station_id || null, date, longitude, latitude]);

        res.status(201).json({ message: "Catalog entry inserted." });
    } catch (err) {
        if (err.code === '23505') {
            return res.status(409).json({
                error: "Duplicate entry",
                detail: err.detail
            });
        }

        console.error("Failed to insert into data_catalog:", err);
        res.status(500).json({ error: "Internal Server Error" });
    }
});

module.exports = router;
