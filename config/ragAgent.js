const { getLLMCompletion } = require('../config/togetherLLM');
const pool = require("../config/postgis");
const clickhouse = require("../config/clickhouse");

function buildPrompt(question, lat, lon) {
    const tableInfo = `
weather_YYYYMMDD(
  "timestamp" DateTime,
  "longitude" Float64,
  "latitude" Float64,
  "sys_country" String,
  "clouds_all" Int8,
  "wind_speed" Float32,
  "wind_deg" Int16,
  "main_pressure" Int16,
  "main_sea_level" Int16,
  "main_grnd_level" Int16,
  "main_temp" Float32,
  "main_temp_min" Float32,
  "main_temp_max" Float32,
  "main_feels_like" Float32,
  "main_humidity" Int8,
  "visibility" Int16,
  "weather_main" String,
  "weather_description" String,
  "weather_icon" String,
  "geom" String
)`;

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

User: "What was the weather like in Jakarta on January 1, 2024?"
SQLQuery: SELECT 
  AVG(\"main_temp\") AS avg_temp,
  AVG(\"main_humidity\") AS avg_humidity,
  AVG(\"wind_speed\") AS avg_wind_speed,
  any(\"weather_main\") AS weather_main,
  any(\"weather_description\") AS weather_description
FROM weather_20240101
WHERE greatCircleDistance(\"latitude\", \"longitude\", -6.1944, 106.8229) < 20000

User: "What was the weather like in Surabaya between January 1 and January 2, 2024?"
SQLQuery: SELECT
  AVG("main_temp") AS avg_temp,
  AVG("main_humidity") AS avg_humidity,
  AVG("wind_speed") AS avg_wind_speed,
  any("weather_main") AS weather_main,
  any("weather_description") AS weather_description
FROM weather_20240101
WHERE greatCircleDistance("latitude", "longitude", -6.1944, 106.8229) < 20000
UNION ALL
SELECT
  AVG("main_temp") AS avg_temp,
  AVG("main_humidity") AS avg_humidity,
  AVG("wind_speed") AS avg_wind_speed,
  any("weather_main") AS weather_main,
  any("weather_description") AS weather_description
FROM weather_20240102
WHERE greatCircleDistance("latitude", "longitude", -6.1944, 106.8229) < 20000

Question: "${question}"
`;
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

function extractSQLFromLLMOutput(llmOutput) {
    const match = llmOutput.match(/SQLQuery:\s*([\s\S]*?)\s*SQLResult:/);

    if (!match || !match[1]) {
        console.error("[ERROR][extractSQL] Failed to extract SQL from LLM output.\n", llmOutput);
        throw new Error("SQLQuery not found in LLM output.");
    }

    let rawSQL = match[1];

    console.log("[DEBUG][extractSQL] Raw SQL from LLM:", rawSQL);

    // Sanitize it
    const cleanedSQL = rawSQL
        .replace(/\\n/g, ' ')
        .replace(/\\"/g, '"')
        .replace(/\\+/g, '')
        .replace(/FORMAT\s+JSON\s*;?/gi, '')  // Remove redundant FORMAT JSON
        .replace(/;\s*$/, '')  // Remove trailing semicolon
        .replace(/\s+/g, ' ')  // Collapse whitespace
        .trim();

    // Validate
    if (!/^SELECT\s/i.test(cleanedSQL)) {
        console.error("[ERROR][extractSQL] Invalid SQL after cleanup:\n", cleanedSQL);
        throw new Error("Cleaned SQL does not start with SELECT.");
    }

    // Namespace patching
    const patchedSQL = cleanedSQL.replace(/\bweather_(\d{8})\b/g, 'weather.weather_$1');

    console.log("[DEBUG][extractSQL] Final SQL to execute:", patchedSQL);

    return patchedSQL;
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
        const rawPrompt = buildPrompt(question, lat, lon, start_date, end_date);
        const llmOutput = await safeGetLLMCompletion(rawPrompt);

        const patchedSQL = extractSQLFromLLMOutput(llmOutput);

        const result = await clickhouse.query({ query: patchedSQL, format: "JSON" });
        const data = await result.json();

        const answerPrompt = buildAnswerPrompt({ question, query: patchedSQL, result: data });
        const finalAnswer = await safeGetLLMCompletion(answerPrompt);

        return finalAnswer;
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
