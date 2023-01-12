const fs = require("fs");
const Pool = require("pg").Pool;

const options = {
  objectMode: true,
  //delimiter: ",",
  quote: null,
  headers: false,
  renameHeaders: false,
};

var data = fs.readFileSync("skill.csv").toLocaleString();

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

  var insertCount =0;
  
async function insertNewState(data,client) {
  

  const queryTotalState = "SELECT COUNT(*) FROM skill";
  const totalState = await client.query(queryTotalState);
  console.log(totalState.rows[0].count)
  
  insertCount = totalState.rows[0].count;
  insertCount++;
  var rowCount=0;
  let skills = []
  // STRING TO ARRAY
  var rows = data.split("\n"); // SPLIT ROWS
  rows.forEach((row) => {
    //console.log(row)
    console.log("row "+rowCount++)

    if(row!= undefined){
      columns = row.split(","); //SPLIT COLUMNS
      console.log("columns "+columns.length)
      skills.push.apply(skills, columns);
      

        // for (const item of columns) {
        //   try {
        //     if(item!=undefined){
        //       //console.log("item =>",item)
        //       //const result1 = await client.query(findRecord)
        //       //insertItem(item,client)

        //       const findRecord = "SELECT COUNT(*) FROM skill WHERE skill_name='"+item+"'";
        //       const result1 = await client.query(findRecord)
        //       console.log("found " + result1.rows[0].count + " row:", item +" "+insertCount);
        //       if(result1.rows[0].count==0){
        //         //console.log("found and insert " + result1.rows[0].count + " row:", item +" "+insertCount);
              
        //           now = new Date().toISOString()

        //           // async/await
        //           try {
        //             const values = [insertCount++,item,null,null,now,now]
        //             const res = await client.query(text, values)
        //             console.log(res.rows[0])
        //             console.log("Inserted ")
        //           } catch (err) {
        //             console.log(err.stack)
        //           }
        //       }
        //     }
        //   }catch(err){
        //     console.log(err.stack)
        //   }
        // }
    
    }

  })

  console.log(skills.length)
    for (let item of skills) {
        try {
          if(item!=undefined){
            //console.log("item =>",item)
            //const result1 = await client.query(findRecord)
            //insertItem(item,client)

            const findRecord = "SELECT COUNT(*) FROM skill WHERE skill_name='"+item+"'";
            const result1 = await client.query(findRecord)
            console.log("found " + result1.rows[0].count + " row:", item +" "+insertCount);
            if(result1.rows[0].count==0){
              //console.log("found and insert " + result1.rows[0].count + " row:", item +" "+insertCount);
            
                now = new Date().toISOString()

                // async/await
               
                try {
                  var text = 'INSERT INTO skill (id, skill_name,skill_type_id,skill_subtype_id,created_at,updated_at) VALUES ($1, $2,$3,$4,$5,$6)'
                  const values = [insertCount++,item,null,null,now,now]
                  
                  const res = await client.query(text, values)
                  console.log("await ")
                  console.log(res.rows[0])
                  console.log("Inserted ")
                } catch (error) {
                  console.log(error.stack)
                }
            }
          }
        }catch(err){
          console.log(err.stack)
        }
      }
      
  
}

async function insertItem(item,client) {
  const findRecord = "SELECT COUNT(*) FROM skill WHERE skill_name='"+item+"'";
  const result1 = await client.query(findRecord)
  console.log("found " + result1.rows[0].count + " row:", item +" "+insertCount);
  if(result1.rows[0].count==0){
    //console.log("found and insert " + result1.rows[0].count + " row:", item +" "+insertCount);
   
      now = new Date().toISOString()

      // async/await
      try {
        const values = [insertCount++,item,null,null,now,now]
        const res = await client.query(text, values)
        console.log(res.rows[0])
        console.log("Inserted ")
      } catch (err) {
        console.log(err.stack)
      }
  }
}

