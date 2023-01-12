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

fs.createReadStream("talent_email.csv")

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
  const text = 'INSERT INTO talent_email (id, talent_id,email_address,email_type,email_status,is_primary,created_at,updated_at) VALUES ($1, $2,$3,$4,$5,$6,$7,$8)'

  const queryTotalState = "SELECT COUNT(*) FROM talent_email";
  const totalState = await client.query(queryTotalState);
  console.log(totalState.rows[0].count)
  
  var insertCount = totalState.rows[0].count;
  insertCount++;
  var rowCount=0;
  for (let row of data) {
    try{

      now = new Date().toISOString()
      const talent_id_query = "SELECT id FROM talent WHERE person_name='"+ row[0].split(",")[0] +"'";
      const found_id = await client.query(talent_id_query);
      console.log(row[0].split(",")[0],"  ",found_id.rows[0].id)
      
      talent_id = found_id.rows[0].id
      email_address = row[1]
      email_type = row[2]
      email_status = row[3]
      is_primary = null
      created_at = now 
      updated_at = now
      try {
        if(email_address!=''){
          const values = [insertCount++,talent_id,email_address,email_type,email_status,is_primary,created_at,updated_at]
          const res = await client.query(text, values)
          console.log(res.rows[0])
        }
        
      } catch (err) {
        console.log(err.stack)
      }
  
      email_address = row[4]
      email_type = row[5]
      email_status = row[6]
      is_primary = null
      created_at = now 
      updated_at = now
      try {
        if(email_address!=''){
          const values = [insertCount++,talent_id,email_address,email_type,email_status,is_primary,created_at,updated_at]
          const res = await client.query(text, values)
          console.log(res.rows[0])
        }
        
      } catch (err) {
        console.log(err.stack)
      }
  
      email_address = row[7]
      email_type = row[8]
      email_status = row[9]
      is_primary = null
      created_at = now 
      updated_at = now
      try {
        if(email_address!=''){
          const values = [insertCount++,talent_id,email_address,email_type,email_status,is_primary,created_at,updated_at]
          const res = await client.query(text, values)
          console.log(res.rows[0])
        }
        
      } catch (err) {
        console.log(err.stack)
      }

    }catch(ex){
      console.log(ex.stack)
    }
    

    //console.log(insertCount++,person_name,first_name,last_name,photo_url,phone_number,linkedin_url,linkedin_id,facebook_url,facebook_id,twitter_url,twitter_username,github_url,github_username,location_id,summary,is_recruiter,created_at,updated_at)
  }
}

