const fs = require("fs");
const Pool = require("pg").Pool;
const fastcsv = require("fast-csv");

let stream = fs.createReadStream("Degree_12_12_2022.csv");
let csvData = [];
let csvStream = fastcsv
  .parse()
  .on("data", function(data) {
    csvData.push(data);
  })
  .on("end", function() {
    // remove the first line: header
    csvData.shift();

    // create a new connection to the database
    const pool = new Pool({
      host: "hirebeat-profile-instance.c9lge1nbo9rx.us-east-1.rds.amazonaws.com",
      user: "postgres",
      database: "hirebeat_profiles",
      password: "4xrSs3KSskkYb1mIBmTB",
      port: 5432
    });

    const now = new Date().toISOString()

    const query =
      "INSERT INTO degree (id, degree_name,created_at,updated_at) VALUES ($1, $2,'"+now+"','"+now+"')";

    pool.connect((err, client, done) => {
      if (err) throw err;

      try {
        csvData.forEach(row => {
            console.log(query)

          client.query(query, row, (err, res) => {
            if (err) {
              console.log(err.stack);
            } else {
              console.log("inserted " + res.rowCount + " row:", row);
            }
          });

        });
      } finally {
        //done();
      }
    });
  });

stream.pipe(csvStream);