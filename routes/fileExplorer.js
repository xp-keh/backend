const express = require("express");
const router = express.Router();
const multer = require("multer");
const { Client } = require("minio");
const pako = require('pako');
let pdfParse = null;

try {
  const pdfParseModule = require('pdf-parse');
  if (typeof pdfParseModule === 'function') {
    pdfParse = pdfParseModule;
  } else if (pdfParseModule && typeof pdfParseModule.default === 'function') {
    pdfParse = pdfParseModule.default;
  } else if (pdfParseModule && typeof pdfParseModule.parse === 'function') {
    pdfParse = pdfParseModule.parse;
  }
  console.log('[INFO] PDF parsing library loaded successfully');
} catch (err) {
  console.warn('[WARN] pdf-parse not available:', err.message);
  console.warn('[WARN] PDF text extraction will not be available');
}
const sharp = require('sharp');
const upload = multer();

const minioClients = {};

try {
  minioClients.seismic = {
    client: new Client({
      endPoint: process.env.MINIO_CONTAINER,
      port: parseInt(process.env.MINIO_PORT),
      useSSL: false,
      accessKey: process.env.MINIO_SEISMIC_ACCESS_KEY,
      secretKey: process.env.MINIO_SEISMIC_SECRET_KEY,
    }),
    bucket: process.env.MINIO_SEISMIC_BUCKET_NAME,
    displayName: "Seismic Data"
  };
} catch (err) {
  console.log('[WARN][MinIO] Seismic client initialization failed:', err.message);
}

try {
  minioClients.weather = {
    client: new Client({
      endPoint: process.env.MINIO_CONTAINER,
      port: parseInt(process.env.MINIO_PORT),
      useSSL: false,
      accessKey: process.env.MINIO_WEATHER_ACCESS_KEY,
      secretKey: process.env.MINIO_WEATHER_SECRET_KEY,
    }),
    bucket: process.env.MINIO_WEATHER_BUCKET_NAME,
    displayName: "Weather Data"
  };
} catch (err) {
  console.log('[WARN][MinIO] Weather client initialization failed:', err.message);
}

const getMinioConfig = (dataset) => {
  if (!minioClients[dataset]) {
    throw new Error(`Dataset '${dataset}' not found. Available datasets: ${Object.keys(minioClients).join(', ')}`);
  }
  return minioClients[dataset];
};

// --- 1. GET ALL AVAILABLE DATASETS/BUCKETS ---
router.get("/datasets", async (req, res) => {
  try {
    const datasets = [];
    
    if (Object.keys(minioClients).length === 0) {
      return res.json({
        count: 0,
        datasets: [],
        message: "MinIO service not available. Please check configuration and service status."
      });
    }
    
    for (const [key, config] of Object.entries(minioClients)) {
      try {
        const bucketExists = await config.client.bucketExists(config.bucket);
        if (bucketExists) {
          const buckets = await config.client.listBuckets();
          const bucketInfo = buckets.find(b => b.name === config.bucket);
          
          datasets.push({
            id: key,
            name: config.displayName,
            bucketName: config.bucket,
            created: bucketInfo ? bucketInfo.creationDate : null,
            accessible: true
          });
        }
      } catch (err) {
        datasets.push({
          id: key,
          name: config.displayName,
          bucketName: config.bucket,
          accessible: false,
          error: err.message
        });
      }
    }
    
    res.json({
      count: datasets.length,
      datasets
    });
  } catch (err) {
    console.error("Error listing datasets:", err);
    res.status(500).json({ error: err.message });
  }
});

// --- 2. BROWSE FILES IN A DATASET ---
router.get("/browse/:dataset", async (req, res) => {
  try {
    const { dataset } = req.params;
    const { path = "", recursive = "false" } = req.query;
    
    const config = getMinioConfig(dataset);
    const isRecursive = recursive === "true";
    
    const normalizedPath = path && !path.endsWith("/") ? path + "/" : path;
    
    const objectsList = [];
    const folderSet = new Set();
    
    const objectsStream = config.client.listObjectsV2(config.bucket, normalizedPath, isRecursive);
    
    objectsStream.on("data", (obj) => {
      if (!obj || (!obj.name && !obj.prefix) || (obj.name && typeof obj.name !== 'string')) {
        console.warn('[WARN] Skipping object with invalid name:', obj);
        return;
      }

      if (obj.prefix) {
        const folderName = obj.prefix.substring(normalizedPath.length);
        if (folderName && folderName.endsWith('/') && !folderName.slice(0, -1).includes('/')) {
          const cleanFolderName = folderName.slice(0, -1);
          folderSet.add(cleanFolderName);
        }
        return;
      }

      if (!obj.name) return;

      if (isRecursive) {
        objectsList.push({
          name: obj.name,
          size: obj.size,
          lastModified: obj.lastModified,
          type: "file",
          etag: obj.etag
        });
      } else {
        const relativePath = obj.name.substring(normalizedPath.length);
        const pathParts = relativePath.split('/');
        
        if (pathParts.length > 1 && pathParts[0]) {
          folderSet.add(pathParts[0]);
        } else if (pathParts[0]) {
          objectsList.push({
            name: obj.name,
            displayName: pathParts[0],
            size: obj.size,
            lastModified: obj.lastModified,
            type: "file",
            etag: obj.etag
          });
        }
      }
    });
    
    objectsStream.on("error", (err) => {
      console.error("Browse error:", err);
      res.status(500).json({ error: err.message });
    });
    
    objectsStream.on("end", () => {
      const folders = Array.from(folderSet).map(folderName => ({
        name: normalizedPath + folderName + "/",
        displayName: folderName,
        type: "folder"
      }));
      
      const result = {
        dataset,
        currentPath: normalizedPath,
        bucket: config.bucket,
        recursive: isRecursive,
        items: [...folders, ...objectsList].sort((a, b) => {
          if (a.type !== b.type) {
            return a.type === "folder" ? -1 : 1;
          }
          return a.displayName.localeCompare(b.displayName);
        })
      };
      
      res.json(result);
    });
  } catch (err) {
    console.error("Browse error:", err);
    res.status(500).json({ error: err.message });
  }
});

// --- 2.5. EXPLORE DEEP FOLDER STRUCTURE (ENHANCED BROWSING) ---
router.get("/explore/:dataset", async (req, res) => {
  try {
    const { dataset } = req.params;
    const { path = "", maxDepth = "3", showEmpty = "true" } = req.query;
    
    const config = getMinioConfig(dataset);
    const normalizedPath = path && !path.endsWith("/") ? path + "/" : path;
    const maxDepthNum = parseInt(maxDepth) || 3;
    const includeEmpty = showEmpty === "true";
    
    console.log(`[DEBUG] Exploring ${dataset} at path: "${normalizedPath}" with maxDepth: ${maxDepthNum}`);
    
    const allObjects = [];
    const objectsStream = config.client.listObjectsV2(config.bucket, normalizedPath, true);
    
    const objectPromise = new Promise((resolve, reject) => {
      objectsStream.on("data", (obj) => {
        if (obj.name || obj.prefix) {
          allObjects.push(obj);
        }
      });
      
      objectsStream.on("error", reject);
      objectsStream.on("end", resolve);
    });
    
    await objectPromise;
    
    const folderTree = {};
    const fileList = [];
    
    allObjects.forEach(obj => {
      if (obj.prefix) {
        const relativePath = obj.prefix.substring(normalizedPath.length);
        if (relativePath) {
          addToFolderTree(folderTree, relativePath, null, maxDepthNum);
        }
      } else if (obj.name) {
        const relativePath = obj.name.substring(normalizedPath.length);
        if (relativePath) {
          const pathParts = relativePath.split('/');
          const fileName = pathParts.pop();
          const folderPath = pathParts.join('/');
          
          const fileInfo = {
            name: obj.name,
            displayName: fileName,
            size: obj.size,
            lastModified: obj.lastModified,
            type: "file",
            etag: obj.etag,
            relativePath: relativePath,
            folderPath: folderPath || ""
          };
          
          fileList.push(fileInfo);
          
          if (folderPath) {
            addToFolderTree(folderTree, folderPath + '/', fileInfo, maxDepthNum);
          }
        }
      }
    });
    
    const breadcrumbs = buildBreadcrumbs(normalizedPath);
    
    const immediateItems = getImmediateChildren(folderTree, fileList, normalizedPath, includeEmpty);
    
    const result = {
      dataset,
      currentPath: normalizedPath,
      bucket: config.bucket,
      maxDepth: maxDepthNum,
      breadcrumbs,
      tree: folderTree,
      immediateItems,
      totalFiles: fileList.length,
      summary: {
        folders: Object.keys(folderTree).length,
        files: fileList.length,
        depth: calculateMaxDepth(folderTree)
      }
    };
    
    res.json(result);
    
  } catch (err) {
    console.error("Explore error:", err);
    res.status(500).json({ error: err.message });
  }
});

function addToFolderTree(tree, path, fileInfo, maxDepth, currentDepth = 0) {
  if (currentDepth >= maxDepth) return;
  
  const parts = path.split('/').filter(p => p);
  if (parts.length === 0) return;
  
  const currentFolder = parts[0];
  const remainingPath = parts.slice(1).join('/');
  
  if (!tree[currentFolder]) {
    tree[currentFolder] = {
      name: currentFolder,
      type: "folder",
      children: {},
      files: [],
      path: currentFolder
    };
  }
  
  if (fileInfo && remainingPath === '') {
    tree[currentFolder].files.push(fileInfo);
  } else if (remainingPath) {
    addToFolderTree(tree[currentFolder].children, remainingPath + '/', fileInfo, maxDepth, currentDepth + 1);
  }
}

function buildBreadcrumbs(path) {
  if (!path || path === '') return [{ name: 'Root', path: '' }];
  
  const parts = path.split('/').filter(p => p);
  const breadcrumbs = [{ name: 'Root', path: '' }];
  
  let currentPath = '';
  parts.forEach(part => {
    currentPath += part + '/';
    breadcrumbs.push({
      name: part,
      path: currentPath
    });
  });
  
  return breadcrumbs;
}

function getImmediateChildren(tree, fileList, currentPath, includeEmpty) {
  const items = [];
  
  Object.values(tree).forEach(folder => {
    const hasContent = folder.files.length > 0 || Object.keys(folder.children).length > 0;
    if (hasContent || includeEmpty) {
      items.push({
        name: currentPath + folder.name + "/",
        displayName: folder.name,
        type: "folder",
        fileCount: folder.files.length,
        subfolderCount: Object.keys(folder.children).length
      });
    }
  });
  
  fileList.forEach(file => {
    if (file.folderPath === '' || file.folderPath === currentPath.replace(/\/$/, '')) {
      items.push(file);
    }
  });
  
  return items.sort((a, b) => {
    if (a.type !== b.type) {
      return a.type === "folder" ? -1 : 1;
    }
    return a.displayName.localeCompare(b.displayName);
  });
}

function calculateMaxDepth(tree, currentDepth = 0) {
  let maxDepth = currentDepth;
  
  Object.values(tree).forEach(folder => {
    if (Object.keys(folder.children).length > 0) {
      const childDepth = calculateMaxDepth(folder.children, currentDepth + 1);
      maxDepth = Math.max(maxDepth, childDepth);
    }
  });
  
  return maxDepth;
}

// --- 3. READ FILE CONTENT (ENHANCED WITH IMAGES & PDFs) ---
router.get("/read/:dataset", async (req, res) => {
  try {
    const { dataset } = req.params;
    const { file, encoding = "utf8", maxSize = "50", format = "auto", page = "1", maxResponseSize = "10", 
            compress = "false", quality = "80", maxWidth = "", maxHeight = "", preview = "false", 
            gzip = "auto" } = req.query;
    
    if (!file) {
      return res.status(400).json({ error: "File path is required" });
    }
    
    const config = getMinioConfig(dataset);
    const maxSizeBytes = parseInt(maxSize) * 1024 * 1024;
    const maxResponseBytes = parseInt(maxResponseSize) * 1024 * 1024;
    const pageNumber = parseInt(page) || 1;
    const shouldCompress = compress === "true";
    const imageQuality = Math.min(Math.max(parseInt(quality) || 80, 10), 100);
    const maxWidthPx = maxWidth ? parseInt(maxWidth) : null;
    const maxHeightPx = maxHeight ? parseInt(maxHeight) : null;
    const isPreview = preview === "true";
    const useGzip = gzip === "true" || gzip === "auto";
    
    let fileStat;
    try {
      fileStat = await config.client.statObject(config.bucket, file);
    } catch (err) {
      return res.status(404).json({ error: "File not found" });
    }
    
    if (fileStat.size > maxSizeBytes) {
      return res.status(413).json({ 
        error: `File too large. Maximum size: ${maxSize}MB, File size: ${(fileStat.size / 1024 / 1024).toFixed(2)}MB`,
        fileSize: fileStat.size,
        maxSize: maxSizeBytes
      });
    }
    
    const fileExtension = file.toLowerCase().split('.').pop();
    const contentType = fileStat.metaData['content-type'] || '';
    
    const imageExtensions = ['jpg', 'jpeg', 'png', 'gif', 'bmp', 'webp', 'tiff'];
    const pdfExtensions = ['pdf'];
    const unsupportedBinaryExtensions = ['zip', 'rar', 'exe', 'dll', 'mp3', 'mp4', 'avi', 'mov', 'xlsx', 'docx'];
    
    const isImage = imageExtensions.includes(fileExtension) || contentType.startsWith('image/');
    const isPdf = pdfExtensions.includes(fileExtension) || contentType === 'application/pdf';
    const isUnsupportedBinary = unsupportedBinaryExtensions.includes(fileExtension);
    
    if (isUnsupportedBinary) {
      return res.status(400).json({ 
        error: "This binary file type is not supported for reading. Use the download endpoint instead.",
        fileType: fileExtension,
        contentType: contentType,
        suggestion: `Use GET /files/download/${dataset}?file=${encodeURIComponent(file)} to download this file`
      });
    }
    
    const stream = await config.client.getObject(config.bucket, file);
    
    const chunks = [];
    
    stream.on("data", (chunk) => {
      chunks.push(chunk);
    });
    
    stream.on("end", async () => {
      try {
        const buffer = Buffer.concat(chunks);
        let content;
        let type;
        let additionalMetadata = {};
        
        if (isImage) {
          try {
            const imageMetadata = await sharp(buffer).metadata();
            
            additionalMetadata = {
              dimensions: {
                width: imageMetadata.width,
                height: imageMetadata.height
              },
              format: imageMetadata.format,
              space: imageMetadata.space,
              channels: imageMetadata.channels,
              depth: imageMetadata.depth,
              density: imageMetadata.density,
              hasProfile: imageMetadata.hasProfile,
              hasAlpha: imageMetadata.hasAlpha
            };
            
            if (format === 'base64' || format === 'auto') {
              let processedBuffer = buffer;
              let compressionApplied = false;
              
              const originalBase64 = buffer.toString('base64');
              const originalResponseSize = originalBase64.length + 1000;
              
              if (shouldCompress || isPreview || maxWidthPx || maxHeightPx || originalResponseSize > maxResponseBytes) {
                try {
                  let sharpInstance = sharp(buffer);
                  
                  if (maxWidthPx || maxHeightPx || isPreview) {
                    const resizeWidth = isPreview ? Math.min(maxWidthPx || 800, 800) : maxWidthPx;
                    const resizeHeight = isPreview ? Math.min(maxHeightPx || 600, 600) : maxHeightPx;
                    
                    sharpInstance = sharpInstance.resize(resizeWidth, resizeHeight, {
                      fit: 'inside',
                      withoutEnlargement: true
                    });
                    compressionApplied = true;
                  }
                  
                  if (imageMetadata.format === 'jpeg' || fileExtension === 'jpg' || fileExtension === 'jpeg') {
                    sharpInstance = sharpInstance.jpeg({ quality: imageQuality });
                    compressionApplied = true;
                  } else if (imageMetadata.format === 'png' || fileExtension === 'png') {
                    sharpInstance = sharpInstance.png({ 
                      quality: imageQuality,
                      compressionLevel: 9
                    });
                    compressionApplied = true;
                  } else if (imageMetadata.format === 'webp' || fileExtension === 'webp') {
                    sharpInstance = sharpInstance.webp({ quality: imageQuality });
                    compressionApplied = true;
                  }
                  
                  processedBuffer = await sharpInstance.toBuffer();
                  
                  const processedMetadata = await sharp(processedBuffer).metadata();
                  additionalMetadata.dimensions = {
                    width: processedMetadata.width,
                    height: processedMetadata.height
                  };
                  additionalMetadata.compressed = compressionApplied;
                  additionalMetadata.originalSize = buffer.length;
                  additionalMetadata.compressedSize = processedBuffer.length;
                  additionalMetadata.compressionRatio = ((buffer.length - processedBuffer.length) / buffer.length * 100).toFixed(1) + '%';
                  
                } catch (compressionError) {
                  console.warn('[WARN] Image compression failed, using original:', compressionError.message);
                  processedBuffer = buffer;
                }
              }
              
              const base64Content = processedBuffer.toString('base64');
              const estimatedResponseSize = base64Content.length + 1000;
              
              if (estimatedResponseSize > maxResponseBytes) {
                return res.status(413).json({
                  error: `Response would be too large. Estimated size: ${(estimatedResponseSize / 1024 / 1024).toFixed(2)}MB, Maximum: ${maxResponseSize}MB`,
                  suggestion: "Try preview=true for a smaller version, or specify maxWidth/maxHeight, or download the file directly",
                  estimatedSize: estimatedResponseSize,
                  maxSize: maxResponseBytes,
                  compressionOptions: {
                    gzip: "Add gzip=true to enable compression (can reduce size by 70-90%)",
                    preview: "Add preview=true for 800x600 version",
                    resize: "Add maxWidth=400&maxHeight=300 to resize",
                    quality: "Add quality=50 for more compression (10-100)",
                    compress: "Add compress=true for automatic optimization"
                  }
                });
              }
              
              content = {
                base64: base64Content,
                dataUrl: `data:${contentType || 'image/' + fileExtension};base64,${base64Content}`,
                size: processedBuffer.length,
                ...(compressionApplied && {
                  optimized: true,
                  originalSize: buffer.length,
                  compressionRatio: additionalMetadata.compressionRatio
                })
              };
              type = "image";
            } else if (format === 'metadata') {
              content = additionalMetadata;
              type = "image-metadata";
            }
            
          } catch (imageError) {
            return res.status(400).json({ 
              error: "Failed to process image file",
              details: imageError.message 
            });
          }
          
        } else if (isPdf) {
          try {
            if (!pdfParse || typeof pdfParse !== 'function') {
              console.log('[DEBUG] PDF parsing library not available, returning as base64');
              const base64Content = buffer.toString('base64');
              const estimatedResponseSize = base64Content.length + 1000;
              
              if (estimatedResponseSize > maxResponseBytes) {
                return res.status(413).json({
                  error: `PDF response would be too large. Estimated size: ${(estimatedResponseSize / 1024 / 1024).toFixed(2)}MB, Maximum: ${maxResponseSize}MB`,
                  suggestion: "Use the download endpoint to get the full PDF file",
                  estimatedSize: estimatedResponseSize,
                  maxSize: maxResponseBytes,
                  downloadUrl: `/files/download/${dataset}?file=${encodeURIComponent(file)}`,
                  compressionOptions: {
                    gzip: "Add gzip=true to enable compression (can reduce size by 70-90%)",
                    text: "Try format=text for text extraction instead of base64",
                    increaseLimit: "Add maxResponseSize=50 to allow larger responses"
                  }
                });
              }
              
              content = {
                base64: base64Content,
                dataUrl: `data:application/pdf;base64,${base64Content}`,
                size: buffer.length,
                note: "PDF text extraction not available - returning as base64",
                warning: "Install pdf-parse package for text extraction"
              };
              type = "pdf-binary-no-parser";
              
              additionalMetadata = {
                pages: "unknown",
                info: {},
                version: "unknown",
                requestedPage: pageNumber,
                textExtractionAvailable: false
              };
            } else {
              console.log(`[DEBUG] Processing PDF page ${pageNumber} with buffer size:`, buffer.length);
              const pdfData = await pdfParse(buffer);
              
              additionalMetadata = {
                pages: pdfData.numpages || 0,
                info: pdfData.info || {},
                version: pdfData.version || 'unknown',
                requestedPage: pageNumber,
                textExtractionAvailable: true
              };
              
              if (format === 'text' || format === 'auto') {
                const fullText = pdfData.text || '';
                let pageText = fullText;
                
                if (pdfData.numpages > 1 && pageNumber <= pdfData.numpages) {
                  const estimatedCharsPerPage = Math.ceil(fullText.length / pdfData.numpages);
                  const startIndex = (pageNumber - 1) * estimatedCharsPerPage;
                  const endIndex = pageNumber * estimatedCharsPerPage;
                  pageText = fullText.substring(startIndex, endIndex);
                  
                  if (pageNumber < pdfData.numpages) {
                    const lastSentence = pageText.lastIndexOf('.');
                    if (lastSentence > estimatedCharsPerPage * 0.7) {
                      pageText = pageText.substring(0, lastSentence + 1);
                    }
                  }
                }
                
                const maxTextLength = isPreview ? 5000 : 50000;
                let truncated = false;
                if (pageText.length > maxTextLength) {
                  pageText = pageText.substring(0, maxTextLength);
                  truncated = true;
                  
                  const lastSpace = pageText.lastIndexOf(' ');
                  if (lastSpace > maxTextLength * 0.8) {
                    pageText = pageText.substring(0, lastSpace) + '...';
                  } else {
                    pageText += '...';
                  }
                }
                
                content = {
                  text: pageText,
                  page: pageNumber,
                  totalPages: pdfData.numpages || 0,
                  isFirstPage: pageNumber === 1,
                  wordCount: pageText ? pageText.split(/\s+/).filter(w => w.length > 0).length : 0,
                  characterCount: pageText.length,
                  truncated: truncated,
                  ...(truncated && { 
                    note: `Text truncated to ${maxTextLength} characters. Use preview=false for longer text or download the full file.` 
                  }),
                  extractionNote: pageNumber === 1 ? "First page extracted" : "Page content approximated from full text"
                };
                type = "pdf-page-text";
              } else if (format === 'base64') {
                const base64Content = buffer.toString('base64');
                const estimatedResponseSize = base64Content.length + 1000;
                
                if (estimatedResponseSize > maxResponseBytes) {
                  return res.status(413).json({
                    error: `PDF response would be too large. Estimated size: ${(estimatedResponseSize / 1024 / 1024).toFixed(2)}MB, Maximum: ${maxResponseSize}MB`,
                    suggestion: "Use the download endpoint to get the full PDF file, or try format=text for text extraction",
                    estimatedSize: estimatedResponseSize,
                    maxSize: maxResponseBytes,
                    downloadUrl: `/files/download/${dataset}?file=${encodeURIComponent(file)}`,
                    compressionOptions: {
                      gzip: "Add gzip=true to enable compression (can reduce size by 70-90%)",
                      text: "Try format=text for text extraction instead of base64",
                      increaseLimit: "Add maxResponseSize=50 to allow larger responses"
                    }
                  });
                }
                
                content = {
                  base64: base64Content,
                  dataUrl: `data:application/pdf;base64,${base64Content}`,
                  size: buffer.length,
                  note: "Full PDF as base64 (all pages)"
                };
                type = "pdf-binary";
              } else if (format === 'metadata') {
                content = additionalMetadata;
                type = "pdf-metadata";
              }
            }
            
          } catch (pdfError) {
            console.error('[ERROR] PDF processing failed:', pdfError);
            const base64Content = buffer.toString('base64');
            const estimatedResponseSize = base64Content.length + 1000;
            
            if (estimatedResponseSize > maxResponseBytes) {
              return res.status(413).json({
                error: `PDF fallback response would be too large. Estimated size: ${(estimatedResponseSize / 1024 / 1024).toFixed(2)}MB, Maximum: ${maxResponseSize}MB`,
                suggestion: "Use the download endpoint to get the full PDF file",
                originalError: "Text extraction failed",
                estimatedSize: estimatedResponseSize,
                maxSize: maxResponseBytes,
                downloadUrl: `/files/download/${dataset}?file=${encodeURIComponent(file)}`
              });
            }
            
            content = {
              base64: base64Content,
              dataUrl: `data:application/pdf;base64,${base64Content}`,
              size: buffer.length,
              error: "Text extraction failed, returning as binary",
              errorDetails: pdfError.message
            };
            type = "pdf-binary-fallback";
            additionalMetadata = {
              pages: "unknown",
              requestedPage: pageNumber,
              textExtractionAvailable: false,
              error: pdfError.message
            };
          }
          
        } else {
          const data = buffer.toString(encoding);
          
          const maxTextLength = isPreview ? 10000 : 100000;
          let processedData = data;
          let truncated = false;
          
          if (data.length > maxTextLength) {
            processedData = data.substring(0, maxTextLength);
            truncated = true;
            
            const lastNewline = processedData.lastIndexOf('\n');
            if (lastNewline > maxTextLength * 0.8) {
              processedData = processedData.substring(0, lastNewline) + '\n...\n[Content truncated]';
            } else {
              processedData += '\n...\n[Content truncated]';
            }
          }
          
          if (fileExtension === 'json') {
            try {
              content = JSON.parse(processedData);
              type = "json";
            } catch (parseError) {
              content = processedData;
              type = "text";
              if (truncated) {
                content = {
                  raw: processedData,
                  truncated: true,
                  originalSize: data.length,
                  note: "JSON parsing failed on truncated content. Download full file for complete JSON."
                };
                type = "truncated-json";
              }
            }
          } else if (fileExtension === 'csv') {
            const lines = processedData.split('\n').filter(line => line.trim());
            if (lines.length > 0) {
              const headers = lines[0].split(',').map(h => h.trim());
              
              const maxRows = isPreview ? 100 : 1000;
              const dataLines = lines.slice(1);
              const limitedLines = dataLines.slice(0, maxRows);
              const rowsLimited = dataLines.length > maxRows;
              
              const rows = limitedLines.map(line => {
                const values = line.split(',').map(v => v.trim());
                const row = {};
                headers.forEach((header, index) => {
                  row[header] = values[index] || '';
                });
                return row;
              });
              
              content = { 
                headers, 
                rows, 
                totalRows: rows.length,
                ...(rowsLimited && { 
                  limited: true,
                  actualTotalRows: dataLines.length,
                  note: `Showing first ${maxRows} rows of ${dataLines.length} total. Use preview=false for more rows.`
                }),
                ...(truncated && {
                  truncated: true,
                  note: "File content was truncated. Download full file for complete data."
                })
              };
              type = "csv";
            } else {
              content = processedData;
              type = "text";
            }
          } else if (fileExtension === 'xml') {
            content = {
              raw: processedData,
              truncated: truncated,
              ...(truncated && { 
                originalSize: data.length,
                note: "XML content truncated. Download full file for complete XML."
              })
            };
            type = "xml";
          } else if (['log', 'txt'].includes(fileExtension)) {
            const lines = processedData.split('\n');
            const maxLines = isPreview ? 500 : 5000;
            const limitedLines = lines.slice(0, maxLines);
            const linesLimited = lines.length > maxLines;
            
            content = {
              raw: processedData,
              lines: limitedLines,
              lineCount: limitedLines.length,
              ...(linesLimited && {
                limited: true,
                actualLineCount: lines.length,
                note: `Showing first ${maxLines} lines of ${lines.length} total. Use preview=false for more lines.`
              }),
              ...(truncated && {
                truncated: true,
                originalSize: data.length,
                note: "File content was truncated. Download full file for complete content."
              })
            };
            type = "log";
          } else {
            content = {
              raw: processedData,
              truncated: truncated,
              ...(truncated && { 
                originalSize: data.length,
                note: "Text content truncated. Download full file for complete content."
              })
            };
            type = "text";
          }
        }
        
        const responseData = { 
          dataset, 
          bucket: config.bucket, 
          file,
          content,
          type,
          metadata: {
            size: fileStat.size,
            lastModified: fileStat.lastModified,
            contentType: contentType,
            encoding: encoding,
            ...additionalMetadata
          }
        };
        
        const responseString = JSON.stringify(responseData);
        const responseSize = Buffer.byteLength(responseString, 'utf8');
        
        const shouldUseGzip = useGzip && (gzip === "true" || responseSize > maxResponseBytes);
        
        if (shouldUseGzip) {
          try {
            const compressed = pako.gzip(responseString);
            const compressionRatio = ((responseSize - compressed.length) / responseSize * 100).toFixed(1);
            
            console.log(`[INFO] Applied gzip compression: ${(responseSize/1024/1024).toFixed(2)}MB -> ${(compressed.length/1024/1024).toFixed(2)}MB (${compressionRatio}% reduction)`);
            
            res.setHeader('Content-Encoding', 'gzip');
            res.setHeader('Content-Type', 'application/json');
            res.setHeader('Content-Length', compressed.length);
            res.setHeader('X-Original-Size', responseSize);
            res.setHeader('X-Compressed-Size', compressed.length);
            res.setHeader('X-Compression-Ratio', compressionRatio + '%');
            
            res.send(Buffer.from(compressed));
            
          } catch (compressionError) {
            console.error('[ERROR] Gzip compression failed:', compressionError);
            res.json(responseData);
          }
        } else {
          res.json(responseData);
        }
        
      } catch (processError) {
        console.error("Processing error:", processError);
        res.status(500).json({ 
          error: "Failed to process file",
          details: processError.message 
        });
      }
    });
    
    stream.on("error", (err) => {
      console.error("Read stream error:", err);
      res.status(500).json({ error: "Failed to read file" });
    });
  } catch (err) {
    console.error("Read error:", err);
    res.status(500).json({ error: err.message });
  }
});

// --- 4. DOWNLOAD FILE ---
router.get("/download/:dataset", async (req, res) => {
  try {
    const { dataset } = req.params;
    const { file } = req.query;
    
    if (!file) {
      return res.status(400).json({ error: "File path is required" });
    }
    
    const config = getMinioConfig(dataset);
    const stream = await config.client.getObject(config.bucket, file);
    
    const fileName = file.split('/').pop();
    res.setHeader("Content-Disposition", `attachment; filename="${fileName}"`);
    res.setHeader("Content-Type", "application/octet-stream");
    
    stream.pipe(res);
    stream.on("error", (err) => {
      console.error("Download stream error:", err);
      res.status(500).end("Error downloading file");
    });
  } catch (err) {
    console.error("Download error:", err);
    res.status(500).json({ error: err.message });
  }
});

// --- 5. UPLOAD FILE ---
router.post("/upload/:dataset", upload.single("file"), async (req, res) => {
  try {
    const { dataset } = req.params;
    
    if (!req.file) {
      return res.status(400).json({ error: "No file uploaded" });
    }
    
    const config = getMinioConfig(dataset);
    const folder = req.body.path || "";
    const normalizedFolder = folder && !folder.endsWith("/") ? folder + "/" : folder;
    const objectName = `${normalizedFolder}${req.file.originalname}`;
    
    await config.client.putObject(config.bucket, objectName, req.file.buffer, req.file.size);
    
    res.json({ 
      message: `File uploaded successfully to ${config.displayName}`,
      dataset,
      bucket: config.bucket,
      object: objectName,
      size: req.file.size
    });
  } catch (err) {
    console.error("Upload error:", err);
    res.status(500).json({ error: err.message });
  }
});

// --- 6. WRITE JSON DATA ---
router.post("/write/:dataset", async (req, res) => {
  try {
    const { dataset } = req.params;
    const { filename, path = "", ...data } = req.body;
    
    if (!filename) {
      return res.status(400).json({ error: "Filename is required" });
    }
    
    const config = getMinioConfig(dataset);
    const normalizedPath = path && !path.endsWith("/") ? path + "/" : path;
    const objectName = `${normalizedPath}${filename}.json`;
    
    const buffer = Buffer.from(JSON.stringify(data, null, 2), "utf-8");
    await config.client.putObject(config.bucket, objectName, buffer, buffer.length);
    
    res.json({ 
      message: `Data written successfully to ${config.displayName}`,
      dataset,
      bucket: config.bucket,
      object: objectName
    });
  } catch (err) {
    console.error("Write error:", err);
    res.status(500).json({ error: err.message });
  }
});

// --- 7. DELETE FILE ---
router.delete("/delete/:dataset", async (req, res) => {
  try {
    const { dataset } = req.params;
    const { file } = req.query;
    
    if (!file) {
      return res.status(400).json({ error: "File path is required" });
    }
    
    const config = getMinioConfig(dataset);
    await config.client.removeObject(config.bucket, file);
    
    res.json({ 
      message: `File deleted successfully from ${config.displayName}`,
      dataset,
      bucket: config.bucket,
      file
    });
  } catch (err) {
    console.error("Delete error:", err);
    res.status(500).json({ error: err.message });
  }
});

// --- 8. GET FILE METADATA ---
router.get("/stat/:dataset", async (req, res) => {
  try {
    const { dataset } = req.params;
    const { file } = req.query;
    
    if (!file) {
      return res.status(400).json({ error: "File path is required" });
    }
    
    const config = getMinioConfig(dataset);
    const stat = await config.client.statObject(config.bucket, file);
    
    res.json({
      dataset,
      bucket: config.bucket,
      file,
      metadata: {
        size: stat.size,
        lastModified: stat.lastModified,
        etag: stat.etag,
        contentType: stat.metaData['content-type']
      }
    });
  } catch (err) {
    console.error("Stat error:", err);
    res.status(500).json({ error: err.message });
  }
});

module.exports = router;