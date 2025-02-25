var express = require('express');
var router = express.Router();
const clickhouse = require('../config/clickhouse');

router.get('/seismic', async (req, res) => {
    try {
        const database = 'seismic';

        const result = await clickhouse.query({
            query: `SHOW TABLES FROM ${database}`,
            format: 'JSON'
        });

        const seismicTables = await result.json();
        let catalogData = [];

        for (const row of seismicTables.data) {
            const tableName = row.name;

            if (!tableName.startsWith("seismic_")) continue;

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
                query: `
                        SELECT DISTINCT geom
                        FROM ${database}.${tableName}
                    `,
                format: 'JSON'
            });

            const geomData = await geomResult.json();
            const geoms = geomData.data.map(row => row.geom);

            for (const geom of geoms) {
                let spatialExtent = geom;

                const geomTuple = `tuple(${geom[0]}, ${geom[1]})`;

                const countResult = await clickhouse.query({
                    query: `
                            SELECT COUNT(*) AS record_count 
                            FROM ${database}.${tableName} 
                            WHERE geom = ${geomTuple}
                        `,
                    format: 'JSON'
                });

                const countData = await countResult.json();
                const recordCount = countData.data[0]?.record_count || 0;

                catalogData.push({
                    catalog_id: `catalog_${tableName}`,
                    table_name: tableName,
                    source_type: tableName.split("_")[0],
                    partition_date: `${tableName.split("_")[1]}`,
                    columns_info: columnsInfo,
                    spatialExtent: geomTuple,
                    record_count: recordCount,
                    created_at: new Date().toISOString(),
                    updated_at: new Date().toISOString()
                });
            }
        }

        res.json({
            success: true,
            message: "Seismic data catalog updated",
            catalog: catalogData
        });

    } catch (error) {
        res.status(500).json({
            success: false,
            message: 'ClickHouse query failed',
            error: error.message
        });
    }
});

module.exports = router;
