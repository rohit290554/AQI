/**
 * GEE CSV → Firestore Ingestion Script
 * Author: Rohit Saxena
 * Purpose: Air Quality Risk Heatmap
 */

const fs = require('fs');
const csv = require('csv-parser');
const admin = require('firebase-admin');
const path = require('path');

/* ===============================
   CONFIGURATION
================================ */

const SERVICE_ACCOUNT = require('./keys/findmobo-200404-firebase-adminsdk-fbsvc-612ce7acd0.json');
const CSV_FILE = './data/India_Air_Quality_Risk.csv';
const COLLECTION_NAME = 'air_quality';

// Use today's date (ISO)
const TODAY = new Date().toISOString().split('T')[0];

/* ===============================
   FIREBASE INIT
================================ */

admin.initializeApp({
  credential: admin.credential.cert(SERVICE_ACCOUNT)
});

const db = admin.firestore();

/* ===============================
   RISK CLASSIFICATION
================================ */

function classifyRisk(score) {
  if (score <= 25) return 'Low';
  if (score <= 50) return 'Moderate';
  if (score <= 75) return 'High';
  return 'Severe';
}

/* ===============================
   INGESTION LOGIC
================================ */

async function ingestCSV() {
  const batch = db.batch();
  let rowCount = 0;

  console.log('🚀 Starting ingestion...');

  fs.createReadStream(path.resolve(CSV_FILE))
    .pipe(csv())
    .on('data', (row) => {
      try {
        // Adjust field names if needed
        const location =
          row.ADM2_NAME ||
          row.ADM1_NAME ||
          row.city ||
          row.district;

        const rawScore = parseFloat(row.mean);

        if (!location || isNaN(rawScore)) {
          console.warn('⚠️ Skipping invalid row:', row);
          return;
        }

        const score = Math.round(rawScore);
        const risk = classifyRisk(score);

        const docRef = db
          .collection(COLLECTION_NAME)
          .doc(TODAY)
          .collection('locations')
          .doc(location);

        batch.set(docRef, {
          location: location,
          score: score,
          risk: risk,
          date: TODAY,
          source: 'Sentinel-5P',
          updatedAt: admin.firestore.FieldValue.serverTimestamp()
        });

        rowCount++;

      } catch (err) {
        console.error('❌ Error processing row:', err);
      }
    })
    .on('end', async () => {
      await batch.commit();
      console.log(`✅ Ingestion complete: ${rowCount} records added.`);
      process.exit(0);
    });
}

/* ===============================
   RUN
================================ */

ingestCSV();
