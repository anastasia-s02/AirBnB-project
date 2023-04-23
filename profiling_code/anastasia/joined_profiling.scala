// SECTION: Profiling -- joined data 

/**
 * Load dataframe. 
 */
val finalDF = spark.read.format("csv").option("header", "true")
    .option("inferSchema", "true").option("delimiter", ",")
    .option("quote", "\"").option("escape", "\"")
    .load("project/join_output/joined_data.csv")

/**
 * Print total number of records.
 */
println("Total number of records in clean joined dataframe: " + finalDF.count())

/**
 * Find means of numerical columns
 */
val meanRating = finalDF.select(mean("rating_range")).first.getDouble(0)
val meanPrice = finalDF.select(mean("price_range")).first.getDouble(0)
val meanTotalListings = finalDF.select(mean("host_total_listings_count")).first.getDouble(0)
val meanReviews = finalDF.select(mean("number_of_reviews")).first.getDouble(0)
val meanFelony = finalDF.select(mean("felony_crimes")).first.getDouble(0)
val meanNonFelony = finalDF.select(mean("non_felony_crimes")).first.getDouble(0)


/**
 * Find medians of numerical columns.
 */
val medianRating = finalDF.stat.approxQuantile("rating_range", Array(0.5), 0)(0)
val medianPrice = finalDF.stat.approxQuantile("price_range", Array(0.5), 0)(0)
val medianTotalListings = finalDF.stat.approxQuantile("host_total_listings_count", Array(0.5), 0)(0)
val medianReviews = finalDF.stat.approxQuantile("number_of_reviews", Array(0.5), 0)(0)
val medianFelony = finalDF.stat.approxQuantile("felony_crimes", Array(0.5), 0)(0)
val medianNonFelony = finalDF.stat.approxQuantile("non_felony_crimes", Array(0.5), 0)(0)

/**
 * Find modes of numerical columns.
 */
val mode1 = finalDF.groupBy("rating_range").agg(count("*").alias("count")).sort(col("count").desc).select("rating_range").limit(1)
val modeRating = mode1.select("rating_range").first().getDouble(0)
val mode2 = finalDF.groupBy("price_range").agg(count("*").alias("count")).sort(col("count").desc).select("price_range").limit(1)
val modePrice = mode2.select("price_range").first().getInt(0)
val mode3 = finalDF.groupBy("host_total_listings_count").agg(count("*").alias("count")).sort(col("count").desc).select("host_total_listings_count").limit(1)
val modeTotalListings = mode3.select("host_total_listings_count").first().getInt(0)
val mode4 = finalDF.groupBy("number_of_reviews").agg(count("*").alias("count")).sort(col("count").desc).select("number_of_reviews").limit(1)
val modeReviews = mode4.select("number_of_reviews").first().getInt(0)
val mode5 = finalDF.groupBy("felony_crimes").agg(count("*").alias("count")).sort(col("count").desc).select("felony_crimes").limit(1)
val modeFelony = mode5.select("felony_crimes").first().getInt(0)
val mode6 = finalDF.groupBy("non_felony_crimes").agg(count("*").alias("count")).sort(col("count").desc).select("non_felony_crimes").limit(1)
val modeNonFelony = mode6.select("non_felony_crimes").first().getInt(0)


/**
 * Find stds of numerical columns.
 */
val stdRating = finalDF.select(stddev("rating_range")).as[Double].first
val stdPrice = finalDF.select(stddev("price_range")).as[Double].first
val stdTotalListins = finalDF.select(stddev("host_total_listings_count")).as[Double].first
val stdReviews = finalDF.select(stddev("number_of_reviews")).as[Double].first
val stdFelony = finalDF.select(stddev("felony_crimes")).as[Double].first
val stdNonFelony = finalDF.select(stddev("non_felony_crimes")).as[Double].first


/**
 * Print out stats for numerical columns. 
 */
println("rating_range: mean - " + meanRating + ", median - " + medianRating + ", mode - " + modeRating + ", std - " + stdRating)
println("price_range: mean - " + meanPrice + ", median - " + medianPrice + ", mode - " + modePrice + ", std - " + stdPrice)
println("host_total_listings: mean - " + meanTotalListings + ", median - " + medianTotalListings + ", mode - " + modeTotalListings +", std - " + stdTotalListins)
println("number_of_reviews: mean - " + meanReviews + ", median - " + medianReviews + ", mode - " + modeReviews +", std - " + stdReviews)
println("felony_crimes: mean - " + meanFelony + ", median - " + medianFelony + ", mode - " + modeFelony +", std - " + stdFelony)
println("non_felony_crimes: mean - " + meanNonFelony + ", median - " + medianNonFelony + ", mode - " + modeNonFelony +", std - " + stdNonFelony)


/**
 * Find stats for text columns.
 */
val countSuperhost = finalDF.groupBy("superhost").agg(count("*").alias("count")).sort(desc("count"))
val countVerified = finalDF.groupBy("host_verified").agg(count("*").alias("count")).sort(desc("count"))
val countMatch = finalDF.groupBy("neighbourhood_match").agg(count("*").alias("count")).sort(desc("count"))


/**
 * Print out stats for text columns. 
 */
println("Counts of superhost:")
println(countSuperhost.show())
println("Counts of verified hosts:")
println(countVerified.show())
println("Counts of hosts living in apartment neighbourhood:")
println(countMatch.show())
