const { Pool } = require("pg");
const path = require("path");
const fs = require("fs");
const fastcsv = require("fast-csv");
require("dotenv").config();

let csvFile;

try {
    csvFile = process.env.csvFile;
    console.log("database.js: csvFile: " + csvFile);
} catch (error) {
    console.log("Error in: database.js:  " + error.stack);
}

// Create CSV from JSON
const createCSV = async (json) => {
    const scriptName = "database.js: createCSV(): ";
    console.log("in " + scriptName);

    console.log(scriptName + " json.response.length: " + json.response.length);

    let currentID = 1;
    const writableStream = fs.createWriteStream(csvFile, { flags: 'w' });

    try {
        if (json.response.length < 1) {
            console.error(scriptName + "no data available from API");
            process.exit(1);
        } else {
            const delim = ",";
            writableStream.write(
                'id' + delim + 'time_stamp' + delim + 'reg_number' + delim + 'alt' + delim +
                'dir' + delim + 'speed' + delim + 'lat' + delim + 'lng' + delim + 'dep_iata' +
                delim + 'flight_icao' + delim + 'status' + `\n`, 'UTF8'
            );

            for (let i = 0; i < json.response.length; i++) {
                writableStream.write(currentID + delim + getDateTime() + delim + json.response[i].reg_number + delim +
                    json.response[i].alt + delim + json.response[i].dir + delim + json.response[i].speed + delim +
                    json.response[i].lat + delim + json.response[i].lng + delim + json.response[i].dep_iata + delim +
                    json.response[i].flight_icao + delim + json.response[i].status + `\n`, 'UTF8');
                currentID++;
            }

            writableStream.end();
            console.log("Finished writing to CSV file: " + csvFile);
        }
    } catch (error) {
        console.error("Error in: createCSV() Error: " + error.stack);
        process.exit(1);
    } finally {
        console.log(scriptName + " done with createCSV()");
    }
};

// Load CSV to Postgres
const loadCSV2 = async () => {
    console.log(" in loadCSV2() ");

    let stream = fs.createReadStream(csvFile);
    let csvData = [];
    let csvStream = fastcsv
        .parse()
        .on("data", function (data) {
            csvData.push(data);
        })
        .on("end", function () {
            csvData.shift();

            const pool = new Pool({
                database: process.env.targetDB,
                user: process.env.pgUser,
                password: process.env.pgPassword,
                port: process.env.pgPort,
                host: process.env.pgHost,
                max: 2,
                connectionTimeoutMillis: 10000,
                idleTimeoutMillis: 10000
            });

            const query = `INSERT INTO ${process.env.dbTable} (id, time_stamp, reg_number, altitude, direction, speed, latitude, longitude, dep_iata, flight_icao, status) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11);`;

            pool.connect((err, client, done) => {
                if (err) throw err;

                try {
                    csvData.forEach(row => {
                        client.query(query, row, (err, res) => {
                            if (err) {
                                console.log("Error in forEach: " + err.stack);
                            } else {
                                console.log("inserted: " + res.rowCount + " row:", row);
                            }
                        });
                    });
                } catch (error) {
                    console.error("Error in: loadCSV2() Error: " + error.stack);
                } finally {
                    console.log(" done inserting records into database ...");
                    done();
                }
            });
        });

    console.log("calling stream.pipe(csvStream) ...");
    stream.pipe(csvStream);
};

// ✅ Fixed runQueries with declared scriptName
async function runQueries(json) {
    const scriptName = "runQueries(): ";
    console.log(" ----------- starting runQueries() ----------");

    try {
        console.log(scriptName + " calling createCSV()");
        await createCSV(json);
    } catch (error) {
        console.log(" Error with createCSV() ... msg: " + error.stack);
    } finally {
        console.log(" done with createCSV() ...");
    }

    try {
        console.log(scriptName + " +++++++++++++++++++ calling loadCSV()  +++++++++++++++++");
        await loadCSV2();
    } catch (error) {
        console.log(" error with loadCSV() ... msg: " + error.stack);
    } finally {
        console.log(" done with loadCSV() ...");
    }

    console.log(scriptName + " done with runQueries() .............");
}

function getDateTime() {
    const now = new Date();
    const pad = (n) => (n < 10 ? '0' + n : n);
    return `${pad(now.getMonth() + 1)}/${pad(now.getDate())}/${now.getFullYear()} ${pad(now.getHours())}:${pad(now.getMinutes())}:${pad(now.getSeconds())}`;
}

// ✅ Minimal loader for server.js
async function loadCSV() {
    console.log("calling loadCSV() from server.js");
    await loadCSV2();
}

module.exports = {
    runQueries,
    loadCSV
};
