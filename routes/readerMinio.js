const express = require("express");
const router = express.Router();
const { Client } = require("minio");

const minioClient = new Client({
  endPoint: process.env.MINIO_CONTAINER || "node1",
  port: parseInt(process.env.MINIO_PORT || "9000", 10),
  useSSL: false,
  accessKey: process.env.MINIO_READER_ACCESS_KEY,
  secretKey: process.env.MINIO_READER_SECRET_KEY,
});

// --- READ ---
router.get("/read", async (req, res) => {
  const bucketName = req.query.bucket || "weather_bucket";
  const objectName = req.query.file || "sample.json";

  try {
    const stream = await minioClient.getObject(bucketName, objectName);
    let data = "";
    stream.on("data", (chunk) => (data += chunk));
    stream.on("end", () =>
      res.json({
        message: `Read success from ${bucketName} ✅`,
        file: objectName,
        content: data,
      })
    );
    stream.on("error", (err) => {
      console.error("Stream error:", err);
      res.status(500).json({ error: "Stream error" });
    });
  } catch (err) {
    console.error("Read error:", err);
    res.status(500).json({ error: err.message });
  }
});

// --- WRITE (expected to fail) ---
router.post("/write", async (req, res) => {
  const bucketName = req.query.bucket || "weather_bucket";
  const objectName = `unauthorized_${Date.now()}.json`;
  const payload = req.body || { info: "Unauthorized write attempt" };

  try {
    const buffer = Buffer.from(JSON.stringify(payload), "utf-8");
    await minioClient.putObject(bucketName, objectName, buffer, buffer.length);

    res.status(500).json({
      message: `Unexpectedly succeeded ❌ — this user should not have write permission.`,
    });
  } catch (err) {
    console.error("Expected write failure:", err);
    res.status(403).json({
      message: `Write denied to ${bucketName} as expected ✅`,
      error: err.message,
    });
  }
});

module.exports = router;
