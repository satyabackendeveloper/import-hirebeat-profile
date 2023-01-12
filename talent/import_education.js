const fs = require("fs");
const fastCsv = require("fast-csv");
const Pool = require("pg").Pool;

const options = {
  columns: true,
};

const data = [];

fs.createReadStream("education.csv")

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
       //console.log("Hello, this is Mike (example)".replace(/ *\([^)]*\) */g, ""))
    }catch(err){
      console.log(err)
    }
    
    });

});

async function insertNewState(data,client) {
  //const query_employment = 'INSERT INTO talent_employment (id, talent_id,company_id,position_name,position_id,experience_level_id,function_id,seniority_id,location_id,is_current,employment_start_date,employment_end_date,employment_duration_months,employment_type,updated_at,created_at) VALUES ($1, $2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)'
  const query_talent_education = 'INSERT INTO talent_education (id, talent_id,education_id,is_current,education_start_date,education_end_date,education_duration_months,updated_at,created_at) VALUES ($1, $2,$3,$4,$5,$6,$7,$8,$9) RETURNING id'
  const query_education = 'INSERT INTO education (id, degree_id,school_id,field_study_id,major_id,is_current,education_start_date,education_end_date,updated_at,created_at) VALUES ($1, $2,$3,$4,$5,$6,$7,$8,$9,$10) RETURNING id'
  const query_school = 'INSERT INTO school (id, school_name,school_url,school_iso_code,school_category,school_type,updated_at,created_at) VALUES ($1, $2,$3,$4,$5,$6,$7,$8) RETURNING id'

  const queryEducatonCount = "SELECT COUNT(*) FROM talent_education";
  const talEduCount = await client.query(queryEducatonCount);
  console.log(talEduCount.rows[0].count)
  var eduCount = talEduCount.rows[0].count;
  eduCount++;

  const querySchoolCount = "SELECT COUNT(*) FROM school";
  const schoolCountt = await client.query(querySchoolCount);
  console.log(schoolCountt.rows[0].count)
  var schoolCount = schoolCountt.rows[0].count;
  schoolCount++;

  const queryeducationCount = "SELECT COUNT(*) FROM education";
  const educationCountt = await client.query(queryeducationCount);
  console.log(educationCountt.rows[0].count)
  var educationCount = educationCountt.rows[0].count;
  educationCount++;


  var rowCount=1;
  for (let row of data) {
    console.log("row=> ",rowCount++)

    try{
      const talent_id = "SELECT id FROM talent where person_name='"+ row[0].split(",")[0] +"'";
      const found_talent_id = await client.query(talent_id);
      if(found_talent_id.rows[0]!=undefined){
        columns = row[1].split(","); //SPLIT COLUMNS
        for (let school of columns) {
            try{
              school_year = school.match(/\(.*?\)/g)
              school = school.replace(/ *\([^)]*\) */g, "").trim()

              console.log("school=> ",school)
              const find_id = "SELECT id FROM school where school_name='"+ school +"'";
              const found_id = await client.query(find_id);
              console.log(found_id.rows[0])
              if(found_id.rows[0]==undefined){
                school_url = null
                school_iso_code = null
                school_category = null
                school_type = null
                now = new Date().toISOString()
                updated_at = now
                created_at = now
                try {
                  const values = [schoolCount++,school,school_url,school_iso_code,school_category,school_type,now,now]
                  const resSchool = await client.query(query_school, values)
                  console.log("School Inserted ",resSchool.rows[0].id)
  
                  degree_id = null
                  school_id = resSchool.rows[0].id
                  field_study_id = null 
                  major_id = null
                  is_current = null 

                  if(school_year!=null){
                    try{
                      const edu_years = school_year[0].replace('(','').replace(')','').split('-');
                  
                      var start_year = new Date(2016, 11, 17);
                      var end_year = new Date(2016, 11, 17);
    
                      if(edu_years[0]!=undefined)
                          start_year = new Date(edu_years[0], 1, 1);
                      else
                          start_year = null
    
                      if(edu_years[1]!=undefined)
                          end_year = new Date(edu_years[1], 1, 1);
                      else
                          end_year = null
                      
                      education_start_date = start_year
                      education_end_date = end_year
                    }catch(ex1){
                      education_start_date = null
                      education_end_date = null
                      console.log(ex1.stack)
                    }
                    
                  }else{
                    education_start_date = null
                    education_end_date = null
                  }
                  

                  now = new Date().toISOString()
                  updated_at = now 
                  created_at = now
  
                  const valuesEducation = [educationCount++,degree_id,school_id,field_study_id,major_id,is_current,education_start_date,education_end_date,updated_at,created_at]
                  const resEducation = await client.query(query_education, valuesEducation)
                  console.log("Education Inserted ",resEducation.rows[0].id)

                  tal_id = found_talent_id.rows[0].id
                  education_id = resEducation.rows[0].id
                  is_current =null

                  if(school_year!=null){
                    try{
                      const edu_years = school_year[0].replace('(','').replace(')','').split('-');
                  
                      var start_year = new Date(2016, 11, 17);
                      var end_year = new Date(2016, 11, 17);
    
                      if(edu_years[0]!=undefined)
                          start_year = new Date(edu_years[0], 1, 1);
                      else
                          start_year = null
    
                      if(edu_years[1]!=undefined)
                          end_year = new Date(edu_years[1], 1, 1);
                      else
                          end_year = null
                      
                      education_start_date = start_year
                      education_end_date = end_year
                    }catch(ex1){
                      education_start_date = null
                      education_end_date = null
                      console.log(ex1.stack)
                    }
                    
                  }else{
                    education_start_date = null
                    education_end_date = null
                  }

                  education_duration_months = null
                  now = new Date().toISOString()
                  updated_at = now 
                  created_at = now

                  const valuesTalentEducation = [eduCount++,tal_id,education_id,is_current,education_start_date,education_end_date,education_duration_months,updated_at,created_at]
                  const resTalentEducation = await client.query(query_talent_education, valuesTalentEducation)
                  console.log("TalentEducation Inserted ",resTalentEducation.rows[0].id)

                } catch (error) {
                  console.log(error.stack)
                }
              }else{
                //console.log(found_id.rows[0])
                //school_id = found_id.rows[0].id
  
                  degree_id = null
                  school_id = found_id.rows[0].id
                  field_study_id = null 
                  major_id = null
                  is_current = null 

                  if(school_year!=null){
                    try{
                      const edu_years = school_year[0].replace('(','').replace(')','').split('-');
                  
                      var start_year = new Date(2016, 11, 17);
                      var end_year = new Date(2016, 11, 17);
    
                      if(edu_years[0]!=undefined)
                          start_year = new Date(edu_years[0], 1, 1);
                      else
                          start_year = null
    
                      if(edu_years[1]!=undefined)
                          end_year = new Date(edu_years[1], 1, 1);
                      else
                          end_year = null
                      
                      education_start_date = start_year
                      education_end_date = end_year
                    }catch(ex1){
                      education_start_date = null
                      education_end_date = null
                      console.log(ex1.stack)
                    }
                    
                  }else{
                    education_start_date = null
                    education_end_date = null
                  }

                  now = new Date().toISOString()
                  updated_at = now 
                  created_at = now
  
                  const valuesEducation = [educationCount++,degree_id,school_id,field_study_id,major_id,is_current,education_start_date,education_end_date,updated_at,created_at]
                  const resEducation = await client.query(query_education, valuesEducation)
                  console.log("Education Inserted ",resEducation.rows[0].id)

                  tal_id = found_talent_id.rows[0].id
                  education_id = resEducation.rows[0].id
                  is_current =null

                  if(school_year!=null){
                    try{
                      const edu_years = school_year[0].replace('(','').replace(')','').split('-');
                  
                      var start_year = new Date(2016, 11, 17);
                      var end_year = new Date(2016, 11, 17);
    
                      if(edu_years[0]!=undefined)
                          start_year = new Date(edu_years[0], 1, 1);
                      else
                          start_year = null
    
                      if(edu_years[1]!=undefined)
                          end_year = new Date(edu_years[1], 1, 1);
                      else
                          end_year = null
                      
                      education_start_date = start_year
                      education_end_date = end_year
                    }catch(ex1){
                      education_start_date = null
                      education_end_date = null
                      console.log(ex1.stack)
                    }
                    
                  }else{
                    education_start_date = null
                    education_end_date = null
                  }

                  education_duration_months = null
                  now = new Date().toISOString()
                  updated_at = now 
                  created_at = now

                  const valuesTalentEducation = [eduCount++,tal_id,education_id,is_current,education_start_date,education_end_date,education_duration_months,updated_at,created_at]
                  const resTalentEducation = await client.query(query_talent_education, valuesTalentEducation)
                  console.log("TalentEducation Inserted ",resTalentEducation.rows[0].id)
              }
              
            }catch(e1){
              console.log(e1.stack)
            }
        }
      }
      
      
    }catch(e3){
      console.log(e3.stack)
    }
  
  }
}

