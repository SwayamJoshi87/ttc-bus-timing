const express = require("express");
const { Pool } = require("pg");
const xml2js = require("xml2js");

const app = express();

// PostgreSQL connection (No SSL logic)
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
});

async function getClosestStop(lat, lon) {
  const query = `
    SELECT stop_id,
           (6371 * acos(
              cos(radians($1)) * cos(radians(stop_lat)) *
              cos(radians(stop_lon) - radians($2)) +
              sin(radians($1)) * sin(radians(stop_lat))
           )) AS distance
    FROM stops
    ORDER BY distance ASC
    LIMIT 1;
  `;
  const values = [lat, lon];
  const { rows } = await pool.query(query, values);
  if (rows.length === 0) {
    throw new Error("No stops found");
  }
  return rows[0].stop_id;
}

async function fetchPredictions(routeTag, stopId) {
  const apiUrl = `https://webservices.nextbus.com/service/publicXMLFeed?command=predictions&a=ttc&r=${routeTag}&s=${stopId}`;
  const response = await fetch(apiUrl);
  if (!response.ok) {
    throw new Error(`Error fetching predictions: ${response.statusText}`);
  }
  const xmlData = await response.text();
  const parser = new xml2js.Parser({ explicitArray: false });
  return await parser.parseStringPromise(xmlData);
}

function formatPredictionMessage(routeTag, predictions) {
  if (predictions.length === 0) {
    return `No arrival time predictions available for route ${routeTag}.`;
  }

  const first = predictions[0];
  const second = predictions.length > 1 ? predictions[1] : null;

  if (!second) {
    return `Bus ${routeTag} is arriving in ${first.minutes} minutes and ${first.seconds} seconds.`;
  }

  return `Bus ${routeTag} is arriving in ${first.minutes} minutes, followed by another bus in ${second.minutes}.`;
}

app.get("/", async (req, res) => {
  const lat = parseFloat(req.query.lat);
  const lon = parseFloat(req.query.lon);
  const routeTag = req.query.route_tag;

  if (isNaN(lat) || isNaN(lon) || !routeTag) {
    return res.status(400).send("Missing or invalid parameters. Expect lat, lon, and route_tag.");
  }

  try {
    const stopId = await getClosestStop(lat, lon);
    const xmlResult = await fetchPredictions(routeTag, stopId);

    let predictions = [];
    try {
      const predictionsNode = xmlResult.body.predictions;
      if (predictionsNode && predictionsNode.direction) {
        let direction = predictionsNode.direction;
        if (!Array.isArray(direction)) {
          direction = [direction];
        }
        for (const dir of direction) {
          let preds = dir.prediction;
          if (preds) {
            if (!Array.isArray(preds)) {
              preds = [preds];
            }
            preds.slice(0, 2).forEach(pred => {
              if (pred.$ && pred.$.minutes && pred.$.seconds) {
                predictions.push({
                  minutes: pred.$.minutes,
                  seconds: pred.$.seconds
                });
              }
            });
          }
        }
      }
    } catch (parseError) {
      console.error("Error parsing XML predictions:", parseError);
    }

    const message = formatPredictionMessage(routeTag, predictions);
    res.send(message);
  } catch (error) {
    console.error("Error processing request:", error);
    res.status(500).send("Internal Server Error");
  }
});

module.exports = app;
