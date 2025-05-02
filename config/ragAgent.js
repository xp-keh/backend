const { getLLMCompletion } = require('../config/togetherLLM');
const pool = require("../config/postgis");
const clickhouse = require("../config/clickhouse");
const crypto = require('crypto');
const redis = require('../config/redis');
const moment = require('moment');

function getHourlyTableNames(baseName, start, end) {
    const tables = [];
    let current = moment.utc(start);
    const endMoment = moment.utc(end);

    while (current <= endMoment) {
        const table = `${baseName}_${current.format("YYYYMMDD_HHmm")}`;
        tables.push(table);
        current.add(1, 'hour');
    }

    return tables;
}

function buildPrompt(question, lat, lon) {
    const tableInfo = `
        weather_YYYYMMDD_HHmm(
            location String,
            lat Float64,
            lon Float64,
            temp Float64,
            feels_like Float64,
            temp_min Float64,
            temp_max Float64,
            pressure Int32,
            humidity Int32,
            wind_speed Float64,
            wind_deg Int32,
            wind_gust Float64,
            clouds Int32,
            timestamp UInt32,
            dt DateTime64(6),
            dt_format String 
        )
        -- Tables are partitioned hourly, e.g. weather_20250426_0000, weather_20250426_0100, etc.
        -- Use UNION ALL when querying across multiple hours.
    `;

    const spatialFilter = (lat != null && lon != null)
        ? `To filter spatially, use:\ngreatCircleDistance("latitude", "longitude", ${lat}, ${lon}) < 20000`
        : '';

    return `
        Always use the exact following format. Do NOT skip or modify labels:
        Question: "..."
        SQLQuery: SELECT ... ← do NOT wrap this in quotes
        SQLResult: "..."  ← leave empty or fake value if needed
        Answer: "..."  ← your best guess based on the SQLResult

        You are a ClickHouse SQL expert. Given an input question, first create a syntactically correct ClickHouse SQL query to run.

        Use only the following tables:
        ${tableInfo}

        Wrap each column name in double quotes. Use only the columns listed in the schema.
        Avoid hallucinating columns or referencing non-existent tables.

        The column "geom" is a WKT geometry.
        ${spatialFilter}

        The user wants a maximum of 100 results unless specified otherwise.

        Given a user query, generate an optimized SQL query for ClickHouse.
        Ensure you use aggregate functions like AVG, MAX, MIN when retrieving numerical data.

        Example:

        User: "What was the weather like in Jakarta on April 26, 2025 at midnight?"
        SQLQuery: SELECT 
            AVG("temp") AS avg_temp,
            AVG("humidity") AS avg_humidity,
            AVG("wind_speed") AS avg_wind_speed,
            any("clouds") AS cloud_coverage
        FROM weather_20250426_0000
        WHERE greatCircleDistance("lat", "lon", -6.1944, 106.8229) < 20000
            AND "dt" BETWEEN '2025-04-26 00:00:00' AND '2025-04-26 00:59:59'

        User: "What was the weather like in Surabaya between January 1 and January 2, 2024?"
        SQLQuery: SELECT
        AVG("temp") AS avg_temp,
        AVG("humidity") AS avg_humidity,
        AVG("wind_speed") AS avg_wind_speed,
        any("weather_main") AS weather_main,
        any("weather_description") AS weather_description
        FROM weather_20240101_0000
        WHERE greatCircleDistance("lat", "lon", -7.2575, 112.7521) < 20000
        AND "dt" BETWEEN '2024-01-01 00:00:00' AND '2024-01-01 00:59:59'
        UNION ALL
        SELECT
        AVG("temp") AS avg_temp,
        AVG("humidity") AS avg_humidity,
        AVG("wind_speed") AS avg_wind_speed,
        any("weather_main") AS weather_main,
        any("weather_description") AS weather_description
        FROM weather_20240101_0100
        WHERE greatCircleDistance("lat", "lon", -7.2575, 112.7521) < 20000
        AND "dt" BETWEEN '2024-01-01 01:00:00' AND '2024-01-01 01:59:59'
        UNION ALL
        -- (continue for all hourly tables up to '2024-01-02 23:59:59')

        Question: "${question}"
    `;
}

function hashPrompt(prompt) {
    return crypto.createHash('sha256').update(prompt).digest('hex');
}

function buildAnswerPrompt({ question, query, result }) {
    let safeResult;
    try {
        safeResult = JSON.stringify(result);
    } catch (e) {
        safeResult = '[Unserializable result]';
    }

    return `Given this user question, SQL query, and result:\n
Question: ${question}
SQL Query: ${query}
SQL Result: ${safeResult}

Please provide a short, simple summary of the weather based on the result.
Only mention relevant values like temperature (in Kelvin), humidity (in %), and wind speed (in m/s) if available.
Do NOT explain unit conversions. 
Keep it brief, natural, and user-friendly.`;
}

function extractSQLFromLLMOutput(llmOutput, dbName = 'weather_dev_3', start_date, end_date) {
    const match = llmOutput.match(/SQLQuery:\s*([\s\S]*?)(?:\s*SQLResult:|\s*Answer:|$)/);

    if (!match || !match[1]) {
        console.error("[ERROR][extractSQL] Failed to extract SQL from LLM output.\n", llmOutput);
        throw new Error("SQLQuery not found in LLM output.");
    }

    let rawSQL = match[1];

    console.log("[DEBUG][extractSQL] Raw SQL from LLM:", rawSQL);

    const cleanedSQL = rawSQL
        .replace(/\\n/g, ' ')
        .replace(/\\"/g, '"')
        .replace(/\\+/g, '')
        .replace(/FORMAT\s+JSON\s*;?/gi, '')
        .replace(/\bLIMIT\s+\d+\b/i, '')
        .replace(/;\s*$/, '')
        .replace(/\s+/g, ' ')
        .trim();

    if (!/^SELECT\s/i.test(cleanedSQL)) {
        console.error("[ERROR][extractSQL] Invalid SQL after cleanup:\n", cleanedSQL);
        throw new Error("Cleaned SQL does not start with SELECT.");
    }

    const dailyTableMatch = cleanedSQL.match(/\bweather_(\d{8})\b/);
    if (!dailyTableMatch || !start_date || !end_date) {
        const fallbackSQL = cleanedSQL.replace(/\bweather_(\d{8}_\d{4})\b/g, `${dbName}.weather_$1`);
        console.log("[DEBUG][extractSQL] Final SQL to execute (fallback):", fallbackSQL);
        return fallbackSQL;
    }

    const basePattern = /\bweather_(\d{8})\b/g;
    const hourlyTables = getHourlyTableNames('weather', start_date, end_date);

    const unionSQL = hourlyTables.map(tableName => {
        return cleanedSQL.replace(basePattern, `${dbName}.${tableName}`);
    }).join(' UNION ALL ');

    console.log("[DEBUG][extractSQL] Final SQL to execute (hourly):", unionSQL);
    return unionSQL;
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

function sanitizeUnionSQL(rawSQL) {
    const withoutComments = rawSQL.replace(/--.*$/gm, '');
    const parts = withoutComments
        .split(/UNION ALL/gi)
        .map(p => p.trim())
        .filter(p => /^SELECT/i.test(p));

    return parts.join(" UNION ALL ");
}


async function runAgent({ question, lat, lon, start_date, end_date }) {
    try {
        const rawPrompt = buildPrompt(question, lat, lon, start_date, end_date);
        const promptHash = hashPrompt(rawPrompt);

        const cachedSQL = await redis.get(`sql:${promptHash}`);
        const cachedAnswer = await redis.get(`answer:${promptHash}`);
        if (cachedSQL && cachedAnswer) {
            console.log('[CACHE][Hit] Using cached result for prompt.');
            return JSON.parse(cachedAnswer); // already has { status, message }
        }

        const llmOutput = await safeGetLLMCompletion(rawPrompt);
        const patchedSQL = extractSQLFromLLMOutput(llmOutput, 'weather_dev_3', start_date, end_date);
        await redis.set(`sql:${promptHash}`, patchedSQL, { EX: 86400 }); // 1 day TTL

        const cleanedSQL = sanitizeUnionSQL(patchedSQL);
        if (!cleanedSQL.toLowerCase().startsWith("select")) {
            throw { code: "LLM_INVALID_SQL", message: "Sanitized SQL is invalid or empty." };
        }

        console.log("[DEBUG] Cleaned SQL:", cleanedSQL);

        const result = await clickhouse.query({
            query: cleanedSQL,
            format: "JSON"
        });
        const data = await result.json();

        const answerPrompt = buildAnswerPrompt({ question, query: patchedSQL, result: data });
        const finalAnswer = await safeGetLLMCompletion(answerPrompt);

        const answerPayload = {
            status: "success",
            message: finalAnswer
        };

        await redis.set(`answer:${promptHash}`, JSON.stringify(answerPayload), { EX: 86400 });

        return answerPayload;
    } catch (error) {
        console.error("[ERROR][runAgent]", error);

        // Custom error mapping
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
