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

// --- LIST ALL BUCKETS ---
router.get("/buckets", async (req, res) => {
  try {
    const buckets = await minioClient.listBuckets();
    res.json({
      count: buckets.length,
      buckets: buckets.map(b => ({ name: b.name, created: b.creationDate })),
    });
  } catch (err) {
    console.error("Error listing buckets:", err);
    res.status(500).json({ error: err.message });
  }
});

// --- GET LIST ---
router.get("/list", (req, res) => {
  const bucketName = req.query.bucket;
  const objectsList = [];

  const objectsStream = minioClient.listObjectsV2(bucketName, "", true); // true = recursive

  objectsStream.on("data", (obj) => objectsList.push(obj.name));
  objectsStream.on("error", (err) => {
    console.error("List error:", err);
    res.status(500).json({ error: err.message });
  });
  objectsStream.on("end", () => {
    res.json({ bucket: bucketName, files: objectsList });
  });
});

// --- READ ---
router.get("/read", async (req, res) => {
  const bucketName = req.query.bucket;
  const objectName = req.query.file;

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
  const bucketName = req.query.bucket;
  const objectName = `unauthorized_${Date.now()}.json`;
  const payload = req.body;

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
