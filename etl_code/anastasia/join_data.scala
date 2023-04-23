// SECTION: Joining Data 
/**
 * Initial dataset for NYC listings can be found on as15026_nyu_edu hdfs in 'project/listings.csv'
 * 
 * Processed & clean dataset for NYC listings can be found on as15026_nyu_edu hdfs in 'project/output/clean_nyc_listings.csv'
 * 
 * The resulting dataset after running this file can be found in 'project/join_output/joined_data.csv'
 */

/**
 * Load two clean datasets as dataframe.
 */
val dfCrime = spark.read.format("csv").option("header", "true").option("inferSchema", "true").option("delimiter", ",").option("quote", "\"").option("escape", "\"").load("project/clean_crime.csv")
val dfNYC = spark.read.format("csv").option("header", "true").option("inferSchema", "true").option("delimiter", ",").option("quote", "\"").option("escape", "\"").load("project/output/clean_nyc_listings.csv")


/**
 * Clean up & reformat crime data for joining.
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
 * Join dataframes.
 */
val joinedDF = dfNYC.join(dfAgg, Seq("neighbourhood_lower"), "left_outer")

// Drop null values that don't make sense. 
val newDF = joinedDF.na.drop(Seq("felony_crimes"))
val dfComplete = newDF.drop("neighbourhood_lower")

println(dfComplete.show(10))

/**
 * Save the joined dataframe to hdfs. 
 */
//dfComplete.coalesce(1).write.format("csv").option("header", "true").mode("overwrite").save("hdfs://nyu-dataproc-m/user/as15026_nyu_edu/project/join_output")







