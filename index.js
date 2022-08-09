require('dotenv').config();
const fs = require("fs");
const Pool = require("pg").Pool;
const fastcsv = require("fast-csv");
let stream = fs.createReadStream("update.csv");
let csvData = [];

const database_name = "riversol";
const database_host = "rmdy.c4enjditb6kk.us-west-2.rds.amazonaws.com";
const database_pw = process.env.DBPW;

let csvStream = fastcsv
  .parse()
  .on("data", function(data) {
    csvData.push(data);
  })
  .on("end", async function() {
    // remove the first line: header
    csvData.shift();
    // create a new connection to the database
    const pool = new Pool({
      host: database_host,
      user: "postgres",
      database: database_name,
      password: database_pw,
      port: 5432
    });
    let query =
      "INSERT INTO klaviyo_hevo3.webhook_marketing (email,id,phone_number,promo_flow_snc_test,sample_flow_test,teaser_test_fp,teaser_test_snc,__hevo_id,__hevo__ingested_at) VALUES";
    await pool.connect(async (err, client, done) => {
      if (err) throw err;
      try {
        csvData.forEach((row, idx, array) => {
          const comma = idx === array.length - 1 ? '' : ','
          query += `(${row.map(r => r ? isNaN(r) ? `'${r.replace(/'/g, '')}'` : r.replace(/'/g, '') : `''`).join()})${comma}\n`
        });
        await fs.unlink('log.txt', function(err) {
          if(err) {
              console.info("File doesn't exist, won't remove it.");
          }
        });
        await fs.appendFile('log.txt', query, function (err) {
          if (err) {
            console.log('log fail')
          } 
        })
        await client.query(query).then((res) => {
          console.log("inserted " + res.rowCount)
        });
      } finally {
        done();
      }
    });
  });
stream.pipe(csvStream);