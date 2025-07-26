const express = require("express");
const fetch = require("node-fetch");
const request = require("request");
const cors = require("cors");
require("dotenv").config();
const { runQueries } = require('../server/database.js');

const app = express();
const PORT = 8000;

app.use(cors());

const myFlightsAPIKey = process.env.flightsAPIKey;
const nearbyAirportDistance = process.env.nearbyAirportDistance;
const defaultAirportCode = process.env.defaultAirportCode;

console.log("server.js(): myFlightsAPIKey:", myFlightsAPIKey);
console.log("nearby airport distance:", nearbyAirportDistance);

const api_base = "https://airlabs.co/api/v9/";

app.get('/hello', async (req, res) => {
  console.log("Hello to You! API route has been called");
  res.send({ message: "Hello to You" });
});

app.get('/flights', async (req, res) => {
  console.error("/flights is an invalid route");
  res.send("/flights is an invalid route");
});

app.get('/flights/:airport_code', async (req, res) => {
  const scriptName = "server.js: /flights/:airport_code(): ";
  console.log("in " + scriptName);

  try {
    let my_airport_code = req.params.airport_code;
    console.log(scriptName + " airport_code:", my_airport_code);

    if (!my_airport_code || my_airport_code.length < 1) {
      my_airport_code = defaultAirportCode;
      console.log("Missing airport code. Default set to:", my_airport_code);
    }

    const api_url = `${api_base}flights?api_key=${myFlightsAPIKey}&arr_iata=${my_airport_code}`;
    console.log("*API URL:", api_url);

    const fetch_response = await fetch(api_url);
    const json = await fetch_response.json();

    console.log(json);
    res.json(json);  //  send response to client before runQueries

    //  runQueries after response to avoid header conflict
    runQueries(json).catch(err => {
      console.error(scriptName + " Error in runQueries:", err.stack);
    });

    console.log(`${scriptName} done with getFlights airport code: ${my_airport_code}`);
  } catch (error) {
    console.error(scriptName + " Error getting flights for airport:", error.stack);
    if (!res.headersSent) {
      res.status(500).send("Failed to fetch flight data.");
    }
  }
});

app.get('/nearbyAirports/:lat,:lng', async (req, res) => {
  const { lat, lng } = req.params;
  const scriptName = "server.js: /nearbyAirports/:lat,:lng: ";

  const url = `${api_base}nearby?api_key=${myFlightsAPIKey}&lat=${lat}&lng=${lng}&distance=${nearbyAirportDistance}`;
  console.log(scriptName + "Calling AirLabs API:", url);

  try {
    const fetch_response = await fetch(url);
    const json = await fetch_response.json();
    res.json(json);
  } catch (error) {
    console.error(scriptName + "Error fetching nearby airports:", error.stack);
    res.status(500).send("Failed to fetch nearby airports.");
  }
});

app.listen(PORT, '0.0.0.0', function (error) {
  if (error) {
    console.error("Error while starting server", error.stack);
  } else {
    console.log("Node Server is Listening on PORT:", PORT);
  }
});
