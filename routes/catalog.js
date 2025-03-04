var express = require('express');
var router = express.Router();

const clickhouse = require('../config/clickhouse');
const pool = require('../config/postgis');
const moment = require("moment-timezone");

const insertIntoPostGIS = async (catalogData) => {
    const insertQuery = `
        MERGE INTO data_catalog AS target
        USING (SELECT $1 AS table_name, $2 AS source_type, $3::TIMESTAMP WITH TIME ZONE AS partition_date, 
                    $4::JSONB AS columns_info, ST_GeogFromText($5) AS spatial_extent,
                    $6::INTEGER AS record_count, $7::TIMESTAMP WITH TIME ZONE AS created_at, $8::TIMESTAMP WITH TIME ZONE AS updated_at) AS source
        ON target.source_type = source.source_type 
        AND target.partition_date = source.partition_date
        AND target.spatial_extent = source.spatial_extent
        WHEN MATCHED THEN
            UPDATE SET table_name = source.table_name, 
                    columns_info = source.columns_info, 
                    record_count = source.record_count, 
                    updated_at = source.updated_at
        WHEN NOT MATCHED THEN
            INSERT (table_name, source_type, partition_date, columns_info, spatial_extent, record_count, created_at, updated_at)
            VALUES (source.table_name, source.source_type, source.partition_date, source.columns_info, source.spatial_extent, source.record_count, source.created_at, source.updated_at);
    `;

    for (const item of catalogData) {
        await pool.query(insertQuery, [
            item.table_name,
            item.source_type,
            item.partition_date,
            JSON.stringify(item.columns_info),
            item.spatialExtent,
            item.record_count,
            moment().tz("Asia/Jakarta").format("YYYY-MM-DD HH:mm:ssZ"),
            moment().tz("Asia/Jakarta").format("YYYY-MM-DD HH:mm:ssZ")
        ]);
    }
};

const processDatabase = async (database) => {
    try {
        const result = await clickhouse.query({
            query: `SHOW TABLES FROM ${database}`,
            format: 'JSON'
        });

        const tables = await result.json();
        let catalogData = [];

        for (const row of tables.data) {
            const tableName = row.name;
            if (!tableName.startsWith(`${database}_`)) continue;

            const columnResult = await clickhouse.query({
                query: `DESCRIBE TABLE ${database}.${tableName}`,
                format: 'JSON'
            });

            const columnData = await columnResult.json();
            const columnsInfo = columnData.data.map(col => ({
                name: col.name,
                type: col.type
            }));

            const geomResult = await clickhouse.query({
                query: `SELECT DISTINCT geom FROM ${database}.${tableName}`,
                format: 'JSON'
            });

            const geomData = await geomResult.json();
            const geoms = geomData.data.map(row => row.geom);

            const partitionDateStr = tableName.split("_")[1];
            const partitionDate = moment(partitionDateStr, "YYYYMMDD").tz("Asia/Jakarta").format("YYYY-MM-DD HH:mm:ssZ");

            for (const geom of geoms) {
                const countResult = await clickhouse.query({
                    query: `SELECT COUNT(*) AS record_count FROM ${database}.${tableName} WHERE geom = tuple(${geom[0]}, ${geom[1]})`,
                    format: 'JSON'
                });

                const countData = await countResult.json();
                const recordCount = countData.data[0]?.record_count || 0;
                catalogData.push({
                    table_name: tableName,
                    source_type: tableName.split("_")[0],
                    partition_date: partitionDate,
                    columns_info: columnsInfo,
                    spatialExtent: `POINT(${geom[0]} ${geom[1]})`,
                    record_count: recordCount,
                    created_at: moment().tz("Asia/Jakarta").format("YYYY-MM-DD HH:mm:ssZ"),
                    updated_at: moment().tz("Asia/Jakarta").format("YYYY-MM-DD HH:mm:ssZ")
                });
            }
        }

        await insertIntoPostGIS(catalogData);

        return { success: true, message: `${database} data catalog updated in PostGIS` };
    } catch (error) {
        return { success: false, message: `${database} ClickHouse query failed`, error: error.message };
    }
};

router.get('/update-catalog', async (req, res) => {
    try {
        const databases = ['seismic', 'weather'];
        let responses = [];

        for (const db of databases) {
            const result = await processDatabase(db);
            responses.push(result);
        }

        res.json({ success: true, results: responses });
    } catch (error) {
        res.status(500).json({ success: false, message: 'Catalog update failed', error: error.message });
    }
});

module.exports = router;
