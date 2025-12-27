/**
 * GEE CSV → Firestore Ingestion Script
 * Author: Rohit Saxena
 * Purpose: Air Quality Risk Heatmap
 */

'use strict';

const fs = require('fs');
const csv = require('csv-parser');
const admin = require('firebase-admin');
const path = require('path');

/* ===============================
   CONSTANTS
================================ */

// CSV path (repo: ingestion/data/India_Air_Quality_Risk.csv)
const CSV_FILE = path.join(__dirname, 'data', 'India_Air_Quality_Risk.csv');

// Firestore root collection
const COLLECTION_NAME = 'air_quality_risk';

// Date key: YYYY-MM-DD
const TODAY = new Date().toISOString().split('T')[0];

/* ===============================
   FIREBASE INIT (GitHub-safe)
================================ */

if (!process.env.FIREBASE_SERVICE_ACCOUNT) {
  console.error('❌ FIREBASE_SERVICE_ACCOUNT not set');
  process.exit(1);
}

const serviceAccount = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT);

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount)
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
  console.log('📄 CSV path:', CSV_FILE);

  if (!fs.existsSync(CSV_FILE)) {
    console.error(`❌ CSV file not found: ${CSV_FILE}`);
    process.exit(1);
  }

  console.log('🚀 Starting ingestion...');

  // Date-level document (for UI / metadata)
  const dateDocRef = db.collection(COLLECTION_NAME).doc(TODAY);
  await dateDocRef.set(
    {
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      source: 'Sentinel-5P'
    },
    { merge: true }
  );

  let rowCount = 0;
  const batch = db.batch();

  fs.createReadStream(CSV_FILE)
    .pipe(csv())
    .on('data', (row) => {
      try {
        const location =
          row.ADM2_NAME ||
          row.ADM1_NAME ||
          row.city ||
          row.district;

        const rawScore = parseFloat(row.mean);

        if (!location || isNaN(rawScore)) return;

        const score = Math.round(rawScore);
        const risk = classifyRisk(score);

        const docRef = db
          .collection(COLLECTION_NAME)
          .doc(TODAY)
          .collection('locations')
          .doc(location);

        batch.set(docRef, {
          location,
          score,
          risk,
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
    })
    .on('error', (err) => {
      console.error('❌ Stream error:', err);
      process.exit(1);
    });
}

/* ===============================
   RUN
================================ */

ingestCSV();
