const fs = require("fs");
const fastCsv = require("fast-csv");
const Pool = require("pg").Pool;

const options = {
  objectMode: true,
  //delimiter: ",",
  quote: null,
  headers: false,
  renameHeaders: false,
};

const data = [];

fs.createReadStream("skill.csv")

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
  const text = 'INSERT INTO skill (id, skill_name,skill_type_id,skill_subtype_id,created_at,updated_at) VALUES ($1, $2,$3,$4,$5,$6)'

  const queryTotalState = "SELECT COUNT(*) FROM skill";
  const totalState = await client.query(queryTotalState);
  console.log(totalState.rows[0].count)
  
  var insertCount = totalState.rows[0].count;
  insertCount++;
  var rowCount=0;
  for (let row of data) {
    console.log(row[0])
    if(row[0] != undefined && row[0]!=''){
      console.log("row "+rowCount++)

      if(row[0]!=''){

        const line = row[0].toString()
        const skillArray = line.split(",");

        for (let i = 0; i < skillArray.length; i++) {
        
          try {
            if(skillArray[i]!=''){
              const findRecord = "SELECT COUNT(*) FROM skill WHERE skill_name='"+skillArray[i]+"'";
              const result1 = await client.query(findRecord)
              if(result1.rows[0].count==0){
                console.log("found and insert " + result1.rows[0].count + " row:", skillArray[i] +" "+insertCount);
        
                  now = new Date().toISOString()
        
                  // async/await
                  try {
                    const values = [insertCount++,skillArray[i],null,null,now,now]
                    const res = await client.query(text, values)
                    console.log(res.rows[0])
                  } catch (err) {
                    console.log(err.stack)
                  }
              }

            }
            
          }catch(error){
            console.log(error.stack)
          }
          

        }

        
      }
      
      
      
    }
  }
}

