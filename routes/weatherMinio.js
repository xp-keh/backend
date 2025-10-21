const express = require("express");
const router = express.Router();
const { Client } = require("minio");

const minioClient = new Client({
  endPoint: process.env.MINIO_CONTAINER,
  port: process.env.MINIO_PORT,
  useSSL: false,
  accessKey: process.env.MINIO_WEATHER_ACCESS_KEY,
  secretKey: process.env.MINIO_WEATHER_SECRET_KEY,
});

const weatherBucket = process.env.MINIO_WEATHER_BUCKET_NAME

// --- GET LIST ---
router.get("/list", (req, res) => {
  const objectsList = [];

  const objectsStream = minioClient.listObjectsV2(weatherBucket, "", true); // true = recursive

  objectsStream.on("data", (obj) => objectsList.push(obj.name));
  objectsStream.on("error", (err) => {
    console.error("List error:", err);
    res.status(500).json({ error: err.message });
  });
  objectsStream.on("end", () => {
    res.json({ bucket: weatherBucket, files: objectsList });
  });
});

// --- READ ---
router.get("/read", async (req, res) => {
  const objectName = req.query.file;

  try {
    const stream = await minioClient.getObject(weatherBucket, objectName);
    let data = "";
    stream.on("data", (chunk) => (data += chunk));
    stream.on("end", () => res.json({ bucket: weatherBucket, file: objectName, content: JSON.parse(data) }));
    stream.on("error", (err) => {
      console.error("Stream error:", err);
      res.status(500).json({ error: "Stream error" });
    });
  } catch (err) {
    console.error("Read error:", err);
    res.status(500).json({ error: err.message });
  }
});

// --- WRITE ---
router.post("/write", async (req, res) => {
  const objectName = `weather_${Date.now()}.json`;
  const payload = req.body;

  try {
    const buffer = Buffer.from(JSON.stringify(payload), "utf-8");
    await minioClient.putObject(weatherBucket, objectName, buffer, buffer.length);
    res.json({ message: `Write success to ${weatherBucket}`, object: objectName });
  } catch (err) {
    console.error("Write error:", err);
    res.status(500).json({ error: err.message });
  }
});

module.exports = router;
