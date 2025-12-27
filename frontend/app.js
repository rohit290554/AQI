/* ===============================
   Firebase imports
================================ */
import { initializeApp } from "https://www.gstatic.com/firebasejs/10.7.1/firebase-app.js";
import {
  getFirestore,
  collection,
  getDocs
} from "https://www.gstatic.com/firebasejs/10.7.1/firebase-firestore.js";

/* ===============================
   Firebase config
================================ */
const firebaseConfig = {
  apiKey: "AIzaSyDwDNkR0AtuHB5SMZJivNRQ8h_caU_FmBw",
  authDomain: "findmobo-200404.firebaseapp.com",
  projectId: "findmobo-200404"
};

const app = initializeApp(firebaseConfig);
const db = getFirestore(app);

/* ===============================
   Map init
================================ */
const map = L.map("map").setView([22.5, 80], 5);

L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
  attribution: "© OpenStreetMap contributors"
}).addTo(map);

/* ===============================
   Risk → Color
================================ */
function getColor(risk) {
  switch (risk) {
    case "Low": return "#2ecc71";
    case "Moderate": return "#f1c40f";
    case "High": return "#e67e22";
    case "Severe": return "#e74c3c";
    default: return "#bdc3c7";
  }
}

/* ===============================
   Load AQ + GeoJSON
================================ */
async function loadMap() {
  const today = new Date().toISOString().split("T")[0];
  // TEMP override if needed:
  // const today = "2025-12-24";

  /* 1️⃣ Load AQ data */
  const snapshot = await getDocs(
    collection(db, "air_quality", today, "locations")
  );

  const aqData = {};
  snapshot.forEach(doc => {
    aqData[doc.data().location] = doc.data();
  });

  /* 2️⃣ Load districts GeoJSON */
  const res = await fetch("data/india_districts.geojson");
  const districts = await res.json();

  /* 3️⃣ Draw choropleth */
  L.geoJSON(districts, {
    style: feature => {
      const district = feature.properties.NAME_2;
      const data = aqData[district];

      if (!data) {
        return {
          fillColor: "#eeeeee",
          weight: 0.5,
          color: "#999",
          fillOpacity: 0.4
        };
      }

      return {
        fillColor: getColor(data.risk),
        weight: 1,
        color: "#333",
        fillOpacity: 0.75
      };
    },
    onEachFeature: (feature, layer) => {
      const district = feature.properties.NAME_2;
      const state = feature.properties.NAME_1;
      const data = aqData[district];

      if (data) {
        layer.bindPopup(`
          <b>${district}, ${state}</b><br/>
          Score: ${data.score}<br/>
          Risk: ${data.risk}
        `);
      }
    }
  }).addTo(map);
}

loadMap();
document.getElementById("loading").style.display = "none";

