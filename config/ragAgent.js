const { getLLMCompletion } = require('../config/togetherLLM');
const pool = require("../config/postgis");
const clickhouse = require("../config/clickhouse");
const crypto = require('crypto');
const redis = require('../config/redis');
const moment = require('moment');

async function getWeatherTableNamesFromPostGIS(start_time, end_time, longitude, latitude, radius = 25000) {
    const formattedStart = moment.utc(start_time).format('YYYY-MM-DD');
    const formattedEnd = moment.utc(end_time).format('YYYY-MM-DD');

    const client = await pool.connect();
    try {
        const { rows } = await pool.query(`
                SELECT table_name
                FROM data_catalog
                WHERE date BETWEEN $1 AND $2
                AND data_type = 'weather'
                AND ST_DWithin(geom, ST_SetSRID(ST_MakePoint($3, $4), 4326)::geography, $5)
            `, [formattedStart, formattedEnd, longitude, latitude, radius]);
        return rows.map(row => row.table_name);
    } catch (error) {
        console.error("[ERROR][PostGIS] Failed to fetch table names.", error);
        throw error;
    } finally {
        client.release();
    }
}

async function getCityNameFromPostGIS(lon, lat) {
    const client = await pool.connect();
    try {
        const { rows } = await client.query(`
            SELECT city 
            FROM cities
            WHERE longitude = $1 AND latitude = $2
            LIMIT 1
        `, [lon, lat]);
        return rows.length > 0 ? rows[0].city : null;
    } catch (error) {
        console.error("[ERROR][PostGIS] Failed to fetch city name.", error);
        throw error;
    } finally {
        client.release();
    }
}

function hashPrompt(prompt) {
    return crypto.createHash('sha256').update(prompt).digest('hex');
}

function buildAnswerPrompt({ question, query, result, lat, lon, start_date, end_date, cityName }) {
    let safeResult;
    try {
        safeResult = JSON.stringify(result);
    } catch (e) {
        safeResult = '[Unserializable result]';
    }

    const locationInfo = cityName
        ? `The weather in ${cityName} (${lat}, ${lon})`
        : `The weather at coordinates (${lat}, ${lon})`;

    const dateRange = moment.utc(start_date).format("DD-MM-YYYY HH:mm")
        + " to "
        + moment.utc(end_date).format("DD-MM-YYYY HH:mm");

    return `Given this user question, SQL query, and result:\n
        Question: ${question}
        SQL Query: ${query}
        SQL Result: ${safeResult}

        Please summarize the weather data as follows:
        "${locationInfo} from ${dateRange} showed approximately [avg_temp] K temperature, [avg_humidity]% humidity, and [avg_wind_speed] m/s wind speed, with cloud coverage around [cloud_coverage]%."

        Please fill in the brackets with appropriate values from the result. Keep it concise and user-friendly.
    `;
}

async function safeGetLLMCompletion(prompt) {
    try {
        const response = await getLLMCompletion(prompt);
        if (!response || typeof response !== 'string') {
            throw new Error("LLM returned empty or non-string output.");
        }
        return response;
    } catch (error) {
        throw {
            code: error.code || 'LLM_ANSWER_FAILED',
            message: error.message || 'LLM call failed.'
        };
    }
}

async function runAgent({ question, lat, lon, start_date, end_date }) {
    try {
        const tableNames = await getWeatherTableNamesFromPostGIS(start_date, end_date, lon, lat);
        if (tableNames.length === 0) {
            throw { code: "NO_DATA_FOUND", message: "No relevant tables found in PostGIS." };
        }

        const cityName = await getCityNameFromPostGIS(lon, lat);
        console.log("[DEBUG] Nearest city:", cityName);

        const dbName = 'weather_dev_1';
        const unionSQL = tableNames.map(tableName => {
            return `
                SELECT 
                    "temp", 
                    "humidity", 
                    "wind_speed", 
                    "clouds", 
                    "timestamp"
                FROM ${dbName}.${tableName}
                WHERE "timestamp" BETWEEN '${moment.utc(start_date).format("DD-MM-YYYYTHH:mm:ss")}' 
                AND '${moment.utc(end_date).format("DD-MM-YYYYTHH:mm:ss")}'
            `;
        }).join(' UNION ALL ');

        const finalSQL = `
            SELECT
                AVG("temp") AS avg_temp,
                AVG("humidity") AS avg_humidity,
                AVG("wind_speed") AS avg_wind_speed,
                any("clouds") AS cloud_coverage
            FROM (
                ${unionSQL}
            )
            LIMIT 100
        `;

        console.log("[DEBUG] Final SQL to execute:", finalSQL);

        const result = await clickhouse.query({
            query: finalSQL,
            format: "JSON"
        });
        const data = await result.json();

        console.log("[DEBUG][ClickHouse Raw Data]", JSON.stringify(data, null, 2));

        if (Array.isArray(data?.data) && data.data.length === 0) {
            console.warn("[DEBUG][ClickHouse] No weather data returned from ClickHouse for this query.");
        }

        const answerPrompt = buildAnswerPrompt({
            question,
            query: finalSQL,
            result: data,
            lat,
            lon,
            start_date,
            end_date,
            cityName
        });
        const finalAnswer = await safeGetLLMCompletion(answerPrompt);

        const answerPayload = {
            status: "success",
            message: finalAnswer
        };

        const promptHash = hashPrompt(answerPrompt);
        await redis.set(`answer:${promptHash}`, JSON.stringify(answerPayload), { EX: 86400 });

        return answerPayload;
    } catch (error) {
        console.error("[ERROR][runAgent]", error);

        let userMessage = "Something went wrong.";
        switch (error.code) {
            case 'LLM_EMPTY_RESPONSE':
            case 'LLM_INVALID_SQL':
                userMessage = "Sorry, I couldn't understand your question.";
                break;
            case 'QUERY_EXECUTION_ERROR':
                userMessage = "Weather data is currently unavailable or incomplete.";
                break;
            case 'NO_DATA_FOUND':
                userMessage = "No weather data available for that time or location.";
                break;
            case 'LLM_ANSWER_FAILED':
                userMessage = "We got the data, but couldn't generate a summary.";
                break;
        }

        return {
            status: "error",
            code: error.code || 'UNKNOWN_ERROR',
            message: userMessage
        };
    }
}

module.exports = { runAgent };
