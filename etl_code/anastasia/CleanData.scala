// SECTION 1: cleaning
/**
 * Dataset can be found using this link, by downloading dataset "listings.csv.gz" (Detailed Listings Data) for New York, NY: 
 * http://insideairbnb.com/get-the-data/
 * 
 * Additionally, the dataset can be found on my hdfs in 'project/listings.csv'
 * 
 * Processed & clean dataset can be found on my hdfs in 'project/output/clean_nyc_listings.csv'
 * 
 * 
 * Cleaning options used: 
 * text formatting: 
 *      bathroom_num - letter symbols are removed and only numbers are kept
 *      superhost & host_verified - converted from "t" and "f"'s to boolean columns
 *      host_neighborhood  & neighborhood - made lowercase 
 * other formatting:
 *      price_range - round down price to a multiple of 10
 *      rating_range - round down rating to a multiple of 0.05
 * binary column: 
 *      phone_and_email - this column is 'true' if host has passed both phone and email 
 *                        verifications, as indicated in 'host_verifications' column, and 'false' otherwise
 *      neighbourhood_match - this column is 'true' if host neighborhood is the same as neighborhood
 *                        in which apartment is located 
 */

/**
 *  Load initial dataset and save it as a dataframe 
 */
val df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").option("multiLine", true).option("delimiter", ",").option("quote", "\"").option("escape", "\"").load("project/listings.csv")


/**
 * Count number of records
 */
val rowCount = df.count()
println(s"Number of rows in CSV file: $rowCount")


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
	"license", "instant_bookable", "calculated_host_listings_count", 
	"calculated_host_listings_count_entire_homes", "calculated_host_listings_count_private_rooms", 
	"calculated_host_listings_count_shared_rooms", "reviews_per_month", "price_numeric")


/**
 * Create a column with only number of bathrooms, no extra text.
 */
val dfWithBathroomNum = cleanDF.withColumn("bathroom_num", regexp_extract(col("bathrooms_text"), "^(\\d+(\\.\\d+)?)", 1).cast("double"))


/**
 * Create a column with lowercase host_neighborhood.
 */
val dfLower = dfWithBathroomNum.withColumn("host_neighbourhood_lower", lower($"host_neighbourhood"))


/**
 * Create a column with lowercase neighborhood_cleansed.
 */
val dfLower2 = dfLower.withColumn("neighbourhood_lower", lower($"neighbourhood_cleansed"))


/**
 * Create a boolean column that is true when lock is included in amenities and false otherwise. 
 */
val dfLower3 = dfLower2.withColumn("verif_lower", lower($"host_verifications"))
val hasPhoneEmail = when(col("verif_lower").contains("phone") && col("verif_lower").contains("email"), true).otherwise(false)
val dfLock = dfLower3.withColumn("phone_and_email", hasPhoneEmail)


/**
 * Make host_is_superhost boolean instead of string.
 */
val dfSuperhost = dfLock.withColumn("superhost", when(col("host_is_superhost") === "t", true).otherwise(false))


/**
 * Make host_identity_verified boolean instead of string. 
 */
val dfIdentity = dfSuperhost.withColumn("host_verified", when(col("host_identity_verified") === "t", true).otherwise(false))

/**
 * Make a column that is 'true' when neighborhood that host lives in matches the neighborhood in which apartment is located.
 */
val dfMatch = dfIdentity.withColumn("neighbourhood_match", when(col("host_neighbourhood_lower") === col("neighbourhood_lower"), true).otherwise(false))

/**
 * Drop the columns that have been processed and no longer need to be used. 
 */
val finalDF = dfMatch.drop("bathrooms_text", "host_neighbourhood", "neighbourhood_cleansed", 
	"host_verifications", "verif_lower", "host_is_superhost", "host_identity_verified")


/**
 * Print out the preview of clean dataframe -- first 20 rows. 
 */
println(finalDF.show(20))


/**
 * Save the dataframe. 
 */
finalDF.coalesce(1).write.format("csv").option("header", "true").save("hdfs://nyu-dataproc-m/user/as15026_nyu_edu/project/output")


// SECTION 2: analysis

/**
 * Find means of numerical columns
 */
val meanRating = finalDF.select(mean("rating_range")).first.getDouble(0)
val meanPrice = finalDF.select(mean("price_range")).first.getDouble(0)
val meanBeds = finalDF.select(mean("bedrooms")).first.getDouble(0)
val meanBaths = finalDF.select(mean("bathroom_num")).first.getDouble(0)
val meanTotalListings = finalDF.select(mean("host_total_listings_count")).first.getDouble(0)
val meanReviews = finalDF.select(mean("number_of_reviews")).first.getDouble(0)


/**
 * Find medians of numerical columns.
 */
val medianRating = finalDF.stat.approxQuantile("rating_range", Array(0.5), 0)(0)
val medianPrice = finalDF.stat.approxQuantile("price_range", Array(0.5), 0)(0)
val medianBeds = finalDF.stat.approxQuantile("bedrooms", Array(0.5), 0)(0)
val medianBaths = finalDF.stat.approxQuantile("bathroom_num", Array(0.5), 0)(0)
val medianTotalListings = finalDF.stat.approxQuantile("host_total_listings_count", Array(0.5), 0)(0)
val medianReviews = finalDF.stat.approxQuantile("number_of_reviews", Array(0.5), 0)(0)


/**
 * Find modes of numerical columns.
 */
val mode1 = finalDF.groupBy("rating_range").agg(count("*").alias("count")).sort(col("count").desc).select("rating_range").limit(1)
val modeRating = mode1.select("rating_range").first().getDouble(0)
val mode2 = finalDF.groupBy("price_range").agg(count("*").alias("count")).sort(col("count").desc).select("price_range").limit(1)
val modePrice = mode2.select("price_range").first().getInt(0)
val mode3 = finalDF.groupBy("bedrooms").agg(count("*").alias("count")).sort(col("count").desc).select("bedrooms").limit(1)
val modeBeds = mode3.select("bedrooms").first().getInt(0)
val mode4 = finalDF.groupBy("bathroom_num").agg(count("*").alias("count")).sort(col("count").desc).select("bathroom_num").limit(1)
val modeBaths = mode4.select("bathroom_num").first().getDouble(0)
val mode5 = finalDF.groupBy("host_total_listings_count").agg(count("*").alias("count")).sort(col("count").desc).select("host_total_listings_count").limit(1)
val modeTotalListings = mode5.select("host_total_listings_count").first().getInt(0)
val mode6 = finalDF.groupBy("number_of_reviews").agg(count("*").alias("count")).sort(col("count").desc).select("number_of_reviews").limit(1)
val modeReviews = mode6.select("number_of_reviews").first().getInt(0)


/**
 * Find stds of numerical columns.
 */
val stdRating = finalDF.select(stddev("rating_range")).as[Double].first
val stdPrice = finalDF.select(stddev("price_range")).as[Double].first
val stdBeds = finalDF.select(stddev("bedrooms")).as[Double].first
val stdBaths = finalDF.select(stddev("bathroom_num")).as[Double].first
val stdTotalListins = finalDF.select(stddev("host_total_listings_count")).as[Double].first
val stdReviews = finalDF.select(stddev("number_of_reviews")).as[Double].first


/**
 * Print out stats for numerical columns. 
 */
println("rating_range: mean - " + meanRating + ", median - " + medianRating + ", mode - " + modeRating + ", std - " + stdRating)
println("price_range: mean - " + meanPrice + ", median - " + medianPrice + ", mode - " + modePrice + ", std - " + stdPrice)
println("bedrooms: mean - " + meanBeds + ", median - " + medianBeds + ", mode - " + modeBeds + ", std - " + stdBeds)
println("bathrooms: mean - " + meanBaths + ", median - " + medianBaths + ", mode - " + modeBaths + ", std - " + stdBaths)
println("host_total_listings: mean - " + meanTotalListings + ", median - " + medianTotalListings + ", mode - " + modeTotalListings +", std - " + stdTotalListins)
println("number_of_reviews: mean - " + meanReviews + ", median - " + medianReviews + ", mode - " + modeReviews +", std - " + stdReviews)


/**
 * Find stats for text columns.
 */
val countNeighbor = finalDF.groupBy("neighbourhood_lower").agg(count("*").alias("count")).sort(desc("count"))
val countHostNeighbor = finalDF.groupBy("host_neighbourhood_lower").agg(count("*").alias("count")).sort(desc("count"))
val countPhoneEmail = finalDF.groupBy("phone_and_email").agg(count("*").alias("count")).sort(desc("count"))
val countSuperhost = finalDF.groupBy("superhost").agg(count("*").alias("count")).sort(desc("count"))
val countVerified = finalDF.groupBy("host_verified").agg(count("*").alias("count")).sort(desc("count"))
val countMatch = finalDF.groupBy("neighbourhood_match").agg(count("*").alias("count")).sort(desc("count"))


/**
 * Print out stats for text columns. 
 */
println("Top 5 neighborhoods:")
println(countNeighbor.show(5))
println("Top 5 host neighborhoods:")
println(countHostNeighbor.show(5))
println("Counts of phone & email verifications:")
println(countPhoneEmail.show())
println("Counts of superhost:")
println(countSuperhost.show())
println("Counts of verified hosts:")
println(countVerified.show())
println("COunts of hosts living in apartment neighbourhood:")
println(countMatch.show())


