const express = require("express");
const router = express.Router();
const multer = require("multer");
const { Client } = require("minio");
const upload = multer(); 

const minioClient = new Client({
  endPoint: process.env.MINIO_CONTAINER,
  port: process.env.MINIO_PORT,
  useSSL: false,
  accessKey: process.env.MINIO_SEISMIC_ACCESS_KEY,
  secretKey: process.env.MINIO_SEISMIC_SECRET_KEY,
});

const seismicBucket = process.env.MINIO_SEISMIC_BUCKET_NAME

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
  const filename = req.body.filename;
  const objectName = `${filename}.json`;
  const payload = req.body;

  try {
    const buffer = Buffer.from(JSON.stringify(payload), "utf-8");
    await minioClient.putObject(seismicBucket, objectName, buffer, buffer.length);
    res.json({ message: `Write success to ${seismicBucket}`, object: objectName });
  } catch (err) {
    console.error("Write error:", err);
    res.status(500).json({ error: err.message });
  }
});

// --- UPLOAD FILE ---
router.post("/upload", upload.single("file"), async (req, res) => {
  if (!req.file) {
    return res.status(400).json({ error: "No file uploaded" });
  }
  const folder = req.body.path || "";
  const normalizedFolder = folder && !folder.endsWith("/") ? folder + "/" : folder;
  const objectName = `${normalizedFolder}${req.file.originalname}`;

  try {
    await minioClient.putObject(seismicBucket, objectName, req.file.buffer, req.file.size);
    res.json({ message: `File uploaded successfully`, object: objectName });
  } catch (err) {
    console.error("Upload error:", err);
    res.status(500).json({ error: err.message });
  }
});

// --- DOWNLOAD FILE ---
router.get("/download", async (req, res) => {
  const objectName = req.query.file;

  if (!objectName) {
    return res.status(400).json({ error: "File name is required" });
  }

  try {
    const stream = await minioClient.getObject(seismicBucket, objectName);

    res.setHeader("Content-Disposition", `attachment; filename="${objectName}"`);
    res.setHeader("Content-Type", "application/octet-stream");

    stream.pipe(res);
    stream.on("error", (err) => {
      console.error("Stream error:", err);
      res.status(500).end("Error downloading file");
    });
  } catch (err) {
    console.error("Download error:", err);
    res.status(500).json({ error: err.message });
  }
});


module.exports = router;
