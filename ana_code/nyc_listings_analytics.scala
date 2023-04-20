// SECTION 4: Running ALS Regression

// *** IN PROGRESS AS OF NOW

import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.ml.feature.VectorAssembler


//val Array(training, testing) = dfComplete.randomSplit(Array(0.8, 0.2))

//val assembler = new VectorAssembler().setInputCols(Array("host_total_listings_count", "bedrooms", "price_range", "number_of_reviews", "bathroom_num", "phone_email_bin", "superhost_bin", "host_verified_bin", "neighborhood_match_bin", "non_felony_crimes", "felony_crimes")).setOutputCol("features")

//val trainingData = assembler.transform(training)
//val testingData = assembler.transform(testing)

//val als = new ALS().setMaxIter(10).setRegParam(0.01).setUserCol("features").setItemCol("rating_range").setRatingCol("rating_range")
