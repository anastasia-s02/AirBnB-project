// PROJECT STATE UPDATE:
/**
 * We are currently working on running ML algorithm to find out the predictors for rating_range. 
 * Then, we need to make visualizations and finish up a presentation. 
 */

// SECTION 3: Processing Crime Data 
/**
 * Initial dataset for NYC listings can be found on as15026_nyu_edu hdfs in 'project/listings.csv'
 * 
 * Processed & clean dataset for NYC listings can be found on as15026_nyu_edu hdfs in 'project/output/clean_nyc_listings.csv'
 * 
 * The resulting dataset after running this file can be found in 'project/join_output/joined_data.csv'
 */

// Note: The files with code will likely be restructured for coherence. 

/**
 * Load two clean datasets as dataframe
 */
val dfCrime = spark.read.format("csv").option("header", "true").option("inferSchema", "true").option("delimiter", ",").option("quote", "\"").option("escape", "\"").load("project/clean_crime.csv")
val dfNYC = spark.read.format("csv").option("header", "true").option("inferSchema", "true").option("delimiter", ",").option("quote", "\"").option("escape", "\"").load("project/output/clean_nyc_listings.csv")


/**
 * Further clean up & reformat crime data.
 */
// Drop all unnecessary columns
val dfCrime2 = dfCrime.drop("CMPLNT_FR_DT", "CMPLNT_TO_DT", "CRM_ATPT_CPTD_CD", "JURISDICTION_CODE", "KY_CD", "LOC_OF_OCCUR_DESC", "OFNS_DESC", "PD_CD", "PD_DESC", "PREM_TYP_DESC", "SUSP_RACE", "SUSP_SEX", "VIC_AGE_GROUP", "VIC_RACE", "VIC_SEX", "Lat_Lon", "Zip_Codes", "Police_Precincts", "BORO_NM", "LAW_CAT_CD")

// Count the total number of felony and non-felony crimes and group them by neighborhood
val dfFelony = dfCrime2.groupBy("Neighborhood").agg(count(when(col("binary_col") === 0, true)).as("non_felony_crimes"), count(when(col("binary_col") === 1, true)).as("felony_crimes"))

// Split rows that have lists in them into separate rows. 
val dfFelony2 = dfFelony.select(col("non_felony_crimes"), col("felony_crimes"), explode(split(col("Neighborhood"), "\\s*,\\s*")) as "neighborhood")
val dfFelony3 = dfFelony2.select(col("non_felony_crimes"), col("felony_crimes"), explode(split(col("neighborhood"), "[,\\s]+and\\s+")) as "neighborhood")
val df4 = dfFelony3.withColumn("neighborhood_clean", regexp_replace(col("neighborhood"), "\\band\\b", ""))
val df5 = df4.withColumn("neighbourhood_lower", lower(col("neighborhood_clean")))
val df6 = df5.drop("neighborhood", "neighborhood_clean")
val dfAgg = df6.groupBy("neighbourhood_lower").agg(sum("non_felony_crimes").as("non_felony_crimes"), sum("felony_crimes").as("felony_crimes"))

/**
 * Further clean up & reformat NYC data -- transform "true/false" columns into 0/1 columns. 
 */
val NYC2 = dfNYC.withColumn("phone_email_bin", when(col("phone_and_email"), 1).otherwise(0)).withColumn("superhost_bin", when(col("superhost"), 1).otherwise(0)).withColumn("host_verified_bin", when(col("host_verified"), 1).otherwise(0)).withColumn("neighborhood_match_bin", when(col("neighbourhood_match"), 1).otherwise(0))
val dfNYC3 = NYC2.drop("phone_and_email", "superhost", "host_verified", "neighbourhood_match", "host_neighbourhood_lower")


/**
 * Join dataframes.
 */
val joinedDF = dfNYC3.join(dfAgg, Seq("neighbourhood_lower"), "left_outer")

// Drop null values that don't make sense. 
val newDF = joinedDF.na.drop(Seq("felony_crimes"))
val dfComplete0 = newDF.na.drop(Seq("rating_range"))
val dfComplete = dfComplete0.drop("neighbourhood_lower")

/**
 * Save the joined dataframe to hdfs. 
 */
//dfComplete.coalesce(1).write.format("csv").option("header", "true").save("hdfs://nyu-dataproc-m/user/as15026_nyu_edu/project/join_output")


// SECTION 4: Running ALS Regression

// *** IN PROGRESS AS OF NOW

import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.ml.feature.VectorAssembler


//val Array(training, testing) = dfComplete.randomSplit(Array(0.8, 0.2))

//val assembler = new VectorAssembler().setInputCols(Array("host_total_listings_count", "bedrooms", "price_range", "number_of_reviews", "bathroom_num", "phone_email_bin", "superhost_bin", "host_verified_bin", "neighborhood_match_bin", "non_felony_crimes", "felony_crimes")).setOutputCol("features")

//val trainingData = assembler.transform(training)
//val testingData = assembler.transform(testing)

//val als = new ALS().setMaxIter(10).setRegParam(0.01).setUserCol("features").setItemCol("rating_range").setRatingCol("rating_range")





