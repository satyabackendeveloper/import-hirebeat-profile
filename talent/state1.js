const fs = require("fs");
const Pool = require("pg").Pool;
const fastcsv = require("fast-csv");
const { resolve } = require("path");


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
      "INSERT INTO state (id, state_name,state_code,region_id,created_at,updated_at) VALUES ($1, $2,$3,$4,'"+now+"','"+now+"')";

    pool.connect((err, client, done) => {
      if (err) throw err;

      try {

        const query_count ="SELECT COUNT(*) FROM state";
        //const count = client.query(query_count);
       // console.log(count);
        client.query(query_count, (err, res) => {
          if (err) {
            console.log(err.stack);
          } else {

            console.log("inserted " + res );
          }
        });
      } finally {
        done();
      }

    });

    function readCsv(path, options, ) {
      return new Promise((resolve, reject) => {
        const data = [];
    
        csv
          .parseFile(path, options)
          .on("error", reject)
          .on("data", (row) => { data.push(row);
          })
          .on("end", () => {
            resolve(data);
          });
      });
    }
    
    async function doThings() {
      const data = await readCsv(
        "Talent_12_13_2022_SGL_18.1K",
        { skipRows: 1 },
      );
      // use data in API...
    }