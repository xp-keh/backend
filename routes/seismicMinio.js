const express = require("express");
const router = express.Router();
const { Client } = require("minio");

const minioClient = new Client({
  endPoint: process.env.MINIO_CONTAINER,
  port: process.env.MINIO_PORT,
  useSSL: false,
  accessKey: process.env.MINIO_SEISMIC_ACCESS_KEY,
  secretKey: process.env.MINIO_SEISMIC_SECRET_KEY,
});

const seismicBucket = process.env.MINIO_SEISMIC_BUCKET_NAME

// --- GET LIST ---
router.get("/list", (req, res) => {
  const objectsList = [];

  const objectsStream = minioClient.listObjectsV2(seismicBucket, "", true); // true = recursive

  objectsStream.on("data", (obj) => objectsList.push(obj.name));
  objectsStream.on("error", (err) => {
    console.error("List error:", err);
    res.status(500).json({ error: err.message });
  });
  objectsStream.on("end", () => {
    res.json({ bucket: seismicBucket, files: objectsList });
  });
});

// --- READ ---
router.get("/read", async (req, res) => {
  const objectName = req.query.file || "seismic_sample.json";

  try {
    const stream = await minioClient.getObject(seismicBucket, objectName);
    let data = "";
    stream.on("data", (chunk) => (data += chunk));
    stream.on("end", () => res.json({ bucket: seismicBucket, file: objectName, content: JSON.parse(data) }));
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
  const objectName = `seismic_${Date.now()}.json`;
  const payload = req.body || { info: "sample seismic write" };

  try {
    const buffer = Buffer.from(JSON.stringify(payload), "utf-8");
    await minioClient.putObject(seismicBucket, objectName, buffer, buffer.length);
    res.json({ message: `Write success to ${seismicBucket}`, object: objectName });
  } catch (err) {
    console.error("Write error:", err);
    res.status(500).json({ error: err.message });
  }
});

module.exports = router;
