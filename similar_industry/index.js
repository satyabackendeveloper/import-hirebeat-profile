const fs = require("fs");
const fastCsv = require("fast-csv");
const Pool = require("pg").Pool;

const options = {
  columns: true,
  //objectMode: true,
  //delimiter: ",",
  //quote: null,
  //headers: true,
  //renameHeaders: false,
};

const data = [];

fs.createReadStream("Industry_12_12_payscale_similar.csv")

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
  const query_product = 'INSERT INTO similar_industry (id, similar_insutry_name, created_at, updated_at ) VALUES ($1, $2,$3,$4) RETURNING id';

  const queryTotalState = "SELECT id FROM similar_industry ORDER BY id DESC LIMIT 1";
  const totalState = await client.query(queryTotalState);
  console.log(totalState.rows[0].id)

  var insertCategory = totalState.rows[0].id
  insertCategory++;

  var rowCount=0;
  for (let row of data) {
    try{

      if(row[0]!=''){
      
      console.log("row ",rowCount++)
      
      const pc_id_query = "SELECT id FROM similar_industry WHERE similar_insutry_name='"+ row[1] +"'";
      const pc_found_id = await client.query(pc_id_query);
      console.log("pc_found_id ",pc_found_id.rows[0])
      if(pc_found_id.rows[0]==undefined){
        
        now = new Date().toISOString()
        
        similar_insutry_name = row[1]
        created_at = now 
        updated_at = now
        try {
          const values = [insertCategory++,similar_insutry_name,created_at, updated_at]
          const res = await client.query(query_product, values)
          console.log("similar_industry insertd ",res.rows[0].id)

        } catch (err) {
          console.log(row[0],err.stack)
          
        }

      }
      
    }
      
    }catch(ex){
      console.log(ex.stack)
    }
    

    //console.log(insertCount++,person_name,first_name,last_name,photo_url,phone_number,linkedin_url,linkedin_id,facebook_url,facebook_id,twitter_url,twitter_username,github_url,github_username,location_id,summary,is_recruiter,created_at,updated_at)
  }
}

