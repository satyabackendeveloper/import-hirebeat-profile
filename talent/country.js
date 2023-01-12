const fs = require("fs");
const fastCsv = require("fast-csv");
const Pool = require("pg").Pool;

const options = {
  objectMode: true,
  delimiter: ",",
  quote: null,
  headers: false,
  renameHeaders: false,
};

const data = [];

fs.createReadStream("country.csv")

  .pipe(fastCsv.parse(options))

  .on("error", (error) => {
    console.log(error);
  })

  .on("data", (row) => {
    data.push(row);
  })

  .on("end", (rowCount) => {
    // remove the first line: header
    data.shift();

    console.log(rowCount);
    console.log(data.length);
    //console.log(data[0]);
    //console.log(data);

    // create a new connection to the database
  const pool = new Pool({
    host: "hirebeat-profile-instance.c9lge1nbo9rx.us-east-1.rds.amazonaws.com",
    user: "postgres",
    database: "hirebeat_profiles",
    password: "4xrSs3KSskkYb1mIBmTB",
    port: 5432
  });

    pool.connect((err, client, done) => {
    if (err) throw err;

    try{
        insertNewState(data,client)
       
    }catch(err){
      console.log(err)
    }
    
    });

});

async function insertNewState(data,client) {
  const text = 'INSERT INTO country (id, country_name,created_at,updated_at) VALUES ($1, $2,$3,$4)'

  const queryTotalState = "SELECT COUNT(*) FROM country";
  const totalState = await client.query(queryTotalState);
  console.log(totalState.rows[0].count)
  
  var insertCount = totalState.rows[0].count;
  insertCount++;
  
  for (let row of data) {
    console.log(row[0])
    if(row[0]!=''){
      
      const findRecord = "SELECT COUNT(*) FROM country WHERE country_name='"+row[0]+"'";
      const result1 = await client.query(findRecord)
      if(result1.rows[0].count==0){
        console.log("found and insert " + result1.rows[0].count + " row:", row[0] +" "+insertCount);

          now = new Date().toISOString()

          // async/await
          try {
            const values = [insertCount++,row[0],now,now]
            const res = await client.query(text, values)
            console.log(res.rows[0])
            // { name: 'brianc', email: 'brian.m.carlson@gmail.com' }
          } catch (err) {
            console.log(err.stack)
          }

          // let result2;
          // try{
          //   const values = [insertCount++,row[7],null,null,now,now]
          //  console.log(query_state);
          //  result2 = await client.query(query_state,[insertCount++,row[7],null,null,now,now])
            
          // }catch(err){
          //   console.log(err)
          // }
          // console.log(result2);
          

      }
      
      
    }
  }
}

