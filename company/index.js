const fs = require("fs");
const Pool = require("pg").Pool;
const fastcsv = require("fast-csv");
const async = require('async');

let stream = fs.createReadStream("Company_12_13_2022_GMO_93K.csv");
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
      "INSERT INTO company (id, company_name, industry_id, sector_id, company_product_id, company_website_url, company_domainl, company_linkedin_url, company_facebook_url, company_twitter_url, company_logo_url, company_size_min, company_size_max, company_found_year, company_total_funding_amount, company_latest_funding_amount, company_latest_funding_type, company_latest_funding_date, company_annual_revenue_amount, company_description, company_keywords, company_size, company_review_rating, created_at, updated_at ) VALUES ($1, $2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,'"+now+"','"+now+"')";

    pool.connect((err, client, done) => {
      if (err) throw err;
      let listOfSkipped = []
      try {
        var regExp = /[a-zA-Z]/g;
        let count = 499;
        
        csvData.forEach(row => {

          count++
          let record=[]
          
          record.push(count) //id
          record.push(row[0]) //company_name

          if(row[2]=='computer software'){
            record.push(26) // industry_id
          } else if(row[2]=='information services'){
            record.push(64) //industry_id
          }else if(row[2]=='non-profit organization management'){
            record.push(99) //industry_id
          }

          record.push(null) //sector_id
          record.push(null) // company_product_id

          record.push('') // company_website_url
          record.push(row[1]) // company_domainl
          record.push(row[7]) // company_linkedin_url
          record.push('') // company_facebook_url
          record.push('')  // company_twitter_url
          record.push('') // company_logo_url
          let str = row[6]
          if(regExp.test(str)){
            record.push(0) // company_size_min
            record.push(0) // company_size_max
          }else{
            if(str.includes("-")){
              const arr =  str.split("-")
              record.push(arr[0]) // company_size_min
              record.push(arr[1]) // company_size_max
            }else{
              if(str.includes("+")){
                const arr =  str.split("+")
                record.push(0) // company_size_min
                record.push(arr[0]) // company_size_max
              }else{
                record.push(0) // company_size_min
                record.push(0) // company_size_max
              }

              
            }
          }
          if(row[5]=='')
            record.push(null)
          else
            record.push(row[5]) // company_found_year

          record.push(null) // company_total_funding_amount
          record.push(null) // company_latest_funding_amount
          record.push(null) // company_latest_funding_type
          record.push(null) // company_latest_funding_date
          record.push(null) // company_annual_revenue_amount
          record.push(null) // company_description
          record.push(null) // company_keywords
          
          // company_size
          let size = row[8]
          if(regExp.test(size)){
            record.push(null)
          }else{
            if(size.includes("+")){
              const arr =  size.split("+")
              record.push(arr[0]) 
            }else if(size.includes("-")){
              const arr =  size.split("-")
              record.push(arr[0]) 
            }else{
              record.push(null) 
            }
          }
          
          record.push(null) // company_review_rating
           //console.log(query)
           // console.log(record)
          try {
            client.query(query, record, (err, res) => {
              if (err) {
                console.log(err.stack);
              } else {
                console.log("inserted " + res.rowCount + " row:", row);
              }
            });
            
          }
          catch(err) {
            listOfSkipped.push(record)
          }
          

        });
      } finally {
        //done();
        
      }
    });
  });

stream.pipe(csvStream);


async function writeSkippedRowinFile(filename,skippedRecrords) {
  try {
      const dirname = path.dirname(filename)
      if (!fs.existsSync(dirname)) {
          fs.mkdirSync(dirname)
      }
      filename+''.replace(/\.[^/.]+$/, "json")
    await fs.writeFileSync(filename, JSON.stringify(skippedRecrords));
  } catch (err) {
    console.log(err);
  }
}

// Defining the queue
const queue = async.queue((task, completed) => {
	console.log("Currently Busy Processing Task " + task);
	//tasks.push[i+10]
	// Simulating a Complex task
	setTimeout(()=>{
		// The number of tasks to be processed
		const remaining = queue.length();
		completed(null, {task, remaining});
	}, 1000);

}, 1); // The concurrency value is 1

// Executes the callback when the queue is done processing all the tasks
queue.drain(() => {
	console.log('Successfully processed all items');
})