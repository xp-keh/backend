require('dotenv').config();

const TOGETHER_API_KEY = process.env.TOGETHER_API_KEY;
const TOGETHER_MODEL = process.env.TOGETHER_LLM_MODEL;

async function getLLMCompletion(prompt) {
    const fetch = (await import('node-fetch')).default;

    const response = await fetch(process.env.TOGETHER_ENDPOINT, {
        method: 'POST',
        headers: {
            'Authorization': `Bearer ${TOGETHER_API_KEY}`,
            'Content-Type': 'application/json'
        },
        body: JSON.stringify({
            model: TOGETHER_MODEL,
            messages: [
                { role: 'system', content: 'You are a helpful SQL assistant.' },
                { role: 'user', content: prompt }
            ],
            temperature: 0.0
        })
    });

    if (!response.ok) {
        const error = await response.text();
        throw new Error(`Together AI Error: ${error}`);
    }

    const data = await response.json();
    return data.choices[0].message.content;
}

module.exports = { getLLMCompletion };
