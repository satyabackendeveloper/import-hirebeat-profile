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

fs.createReadStream("talent.csv")

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
  const text = 'INSERT INTO talent (id, person_name,first_name,last_name,photo_url,phone_number,linkedin_url,linkedin_id,facebook_url,facebook_id,twitter_url,twitter_username,github_url,github_username,location_id,summary,is_recruiter,created_at,updated_at) VALUES ($1, $2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19)'

  const queryTotalState = "SELECT COUNT(*) FROM talent";
  const totalState = await client.query(queryTotalState);
  console.log(totalState.rows[0].count)
  
  var insertCount = totalState.rows[0].count;
  insertCount++;
  var rowCount=0;
  for (let row of data) {
    now = new Date().toISOString()

    person_name = row[0].split(",")[0]
    first_name = row[1].split(",")[0]
    last_name = row[2].split(",")[0]
    photo_url = row[7]
    phone_number = null
    linkedin_url = row[4]
    linkedin_id = null
    facebook_url = row[6]
    facebook_id = null
    twitter_url = row[5]
    twitter_username = null
    github_url = null
    github_username = null
    location_id = null
    summary = row[3].split(",")[0]
    is_recruiter = null
    created_at = now 
    updated_at = now
    try {
      const values = [insertCount++,person_name,first_name,last_name,photo_url,phone_number,linkedin_url,linkedin_id,facebook_url,facebook_id,twitter_url,twitter_username,github_url,github_username,location_id,summary,is_recruiter,created_at,updated_at]
      const res = await client.query(text, values)
      console.log(res.rows[0])
    } catch (err) {
      console.log(err.stack)
    }

    //console.log(insertCount++,person_name,first_name,last_name,photo_url,phone_number,linkedin_url,linkedin_id,facebook_url,facebook_id,twitter_url,twitter_username,github_url,github_username,location_id,summary,is_recruiter,created_at,updated_at)
  }
}

