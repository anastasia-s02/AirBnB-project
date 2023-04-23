// CLEANING
/**
 * Source of the dataset can be found using this link, by downloading dataset "listings.csv.gz" (Detailed Listings Data) for New York, NY as of Dec 4, 2022: 
 * http://insideairbnb.com/get-the-data/
 * 
 * Additionally, the dataset can be found on as15026 hdfs in 'project/listings.csv'
 * 
 * Processed & clean dataset can be found on as15026 hdfs in 'project/output/clean_nyc_listings.csv'
 * 
 * Columns present after cleaning: 
 * price_range - round down price to a multiple of 10
 * rating_range - round down rating to a multiple of 0.05
 * neighbourhood_match - this column is 1 if host neighborhood is the same as neighborhood
 *                        in which apartment is located 
 * neighbourhood_lower - column to be used for joining datasets 
 * number_of_reviews
 * host_total_listings_count
 * host_verified - 1 if host is verified, 0 otherwise
 * superhost - 1 if host is superhost, 0 otherwise
 * 
 */

/**
 *  Load initial dataset and save it as a dataframe 
 */
val df = spark.read.format("csv").option("header", "true")
    .option("inferSchema", "true").option("multiLine", true)
    .option("delimiter", ",").option("quote", "\"").option("escape", "\"")
    .load("project/listings.csv")


/**
 * Count number of records
 */
val rowCount = df.count()
println(s"Number of rows in initial CSV file: $rowCount")


/** 
 * Make a dataframe with a new column "price_range", where price is rounded down 
 * to a multiple of $10 
 */
val rangeSize = 10
val priceRangeDF = df.withColumn("price_numeric", regexp_replace(col("price"), "\\$", "")
		.cast("double")).withColumn("price_range", (when(col("price_numeric") % rangeSize === 0, col("price_numeric"))
		.otherwise((col("price_numeric") / rangeSize).cast("int") * rangeSize)).cast("int").alias("price_range"))


/** 
 * Make a dataframe with a new column "rating_range", where rating is rounded down 
 * to a multiple of 0.05
 */
val rangeSize2 = 0.05
val allRangeDF = priceRangeDF.withColumn("rating_range", round(floor(col("review_scores_rating") / rangeSize2) * rangeSize2, 2))


/**
 * Drop all the columns that we don't need. 
 */
val cleanDF = allRangeDF.drop("id", "listing_url", "scrape_id", "last_scraped", 
	"source", "name", "description", "neighborhood_overview", "picture_url", "host_id", 
	"host_url", "host_name", "host_since", "host_location", "host_about",
	"host_response_time", "host_response_rate", "host_acceptance_rate", "neighbourhood",
	"host_thumbnail_url", "host_picture_url", "host_listings_count", "amenities", 
	"host_has_profile_pic", "neighbourhood_group_cleansed", "latitude", "longitude", 
	"property_type", "room_type", "accommodates", "bathrooms", "price", "review_scores_rating", 
	"beds", "minimum_nights", "maximum_nights", "minimum_minimum_nights", "maximum_minimum_nights", 
	"minimum_maximum_nights", "maximum_maximum_nights", "minimum_nights_avg_ntm", 
	"maximum_nights_avg_ntm", "calendar_updated", "has_availability", "availability_30", 
	"availability_60", "availability_90", "availability_365", "calendar_last_scraped", 
	"number_of_reviews_ltm", "number_of_reviews_l30d", "first_review", "last_review", 
	"review_scores_accuracy", "review_scores_cleanliness", "review_scores_checkin", 
	"review_scores_communication", "review_scores_location", "review_scores_value", 
	"license", "instant_bookable", "calculated_host_listings_count", "bedrooms", "bathrooms_text",
	"calculated_host_listings_count_entire_homes", "calculated_host_listings_count_private_rooms", 
	"calculated_host_listings_count_shared_rooms", "reviews_per_month", "price_numeric")


/**
 * Create a column with only number of bathrooms, no extra text.
 */
val extraColumnsDF = cleanDF.withColumn("host_neighbourhood_lower", lower($"host_neighbourhood"))
    .withColumn("neighbourhood_lower", lower($"neighbourhood_cleansed"))
    .withColumn("superhost", when(col("host_is_superhost") === "t", 1).otherwise(0))
    .withColumn("host_verified", when(col("host_identity_verified") === "t", 1).otherwise(0))

val extraColumnsDF2 = extraColumnsDF.withColumn("neighbourhood_match", when(col("host_neighbourhood_lower") === col("neighbourhood_lower"), 1).otherwise(0))


/**
 * Drop the columns that have been processed and no longer need to be used. 
 */
val finalDF = extraColumnsDF2.drop("host_neighbourhood", "neighbourhood_cleansed", 
	"host_verifications", "verif_lower", "host_is_superhost", "host_identity_verified", "host_neighbourhood_lower")
    .na.drop(Seq("rating_range")).na.drop(Seq("price_range"))


/**
 * Print out the preview of clean dataframe -- first 20 rows. 
 */
println(finalDF.show(20))
println("Number of rows in the processed dataset: " + finalDF.count())

/**
 * Save the dataframe.
 */
//finalDF.coalesce(1).write.format("csv").option("header", "true").mode("overwrite").save("hdfs://nyu-dataproc-m/user/as15026_nyu_edu/project/output")
