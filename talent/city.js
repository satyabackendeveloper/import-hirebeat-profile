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

fs.createReadStream("city.csv")

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
  const text = 'INSERT INTO city (id, city_name,state_id,created_at,updated_at) VALUES ($1, $2,$3,$4,$5)'

  const queryTotalState = "SELECT COUNT(*) FROM city";
  const totalState = await client.query(queryTotalState);
  console.log(totalState.rows[0].count)
  
  var insertCount = totalState.rows[0].count;
  insertCount++;
  var countRow=0;
  for (let row of data) {
    console.log(row[0] + " "+ countRow++)
    if(row[0]!=''){
      
      const findRecord = "SELECT COUNT(*) FROM city WHERE city_name='"+row[0]+"'";
      const result1 = await client.query(findRecord)
      if(result1.rows[0].count==0){
        console.log("new :", row[0] +" "+insertCount +" "+row[1]);

        var stateId = 0
        if(row[1]!=''){
          const findStateId = "SELECT id FROM state WHERE state_name='"+row[1]+"'";
          const state_result = await client.query(findStateId)
          console.log(state_result.rows[0].id)
          stateId = state_result.rows[0].id
        }else{
          stateId = null
        }
        

          now = new Date().toISOString()

          // async/await
          try {
            const values = [insertCount++,row[0],stateId,now,now]
            //console.log(values)
            const res = await client.query(text, values)
            console.log(res.rows[0])
            // { name: 'brianc', email: 'brian.m.carlson@gmail.com' }
          } catch (err) {
            console.log(err.stack)
          }

         

      }
      
      
    }
  }
}

