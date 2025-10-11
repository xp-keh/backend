const express = require("express");
const router = express.Router();
const { Client } = require("minio");

const minioClient = new Client({
  endPoint: process.env.MINIO_ENDPOINT.replace("http://", ""),
  port: 9000,
  useSSL: false,
  accessKey: process.env.MINIO_ACCESS_KEY,
  secretKey: process.env.MINIO_SECRET_KEY,
});

router.get("/read", async (req, res) => {
  const bucketName = req.query.bucket || "seismic";
  const objectName = req.query.file || "test_upload_latest.json";

  try {
    const stream = await minioClient.getObject(bucketName, objectName);
    let data = "";
    stream.on("data", (chunk) => (data += chunk));
    stream.on("end", () => res.json({ message: `Read success from ${bucketName} ✅`, content: data }));
    stream.on("error", (err) => {
      console.error("Stream error:", err);
      res.status(500).json({ error: "Error reading file" });
    });
  } catch (err) {
    console.error("Read error:", err);
    res.status(500).json({ error: err.message });
  }
});

router.post("/write", async (req, res) => {
  const bucketName = req.query.bucket || "seismic";

  try {
    const content = JSON.stringify({ test: "Write attempt", bucket: bucketName });
    const buffer = Buffer.from(content, "utf-8");
    await minioClient.putObject(bucketName, `unauthorized_${Date.now()}.json`, buffer, buffer.length);
    res.json({ message: "Unexpectedly succeeded ❌" });
  } catch (err) {
    console.error("Expected write failure:", err);
    res.status(403).json({ message: `Write denied to ${bucketName} as expected ✅`, error: err.message });
  }
});

module.exports = router;
