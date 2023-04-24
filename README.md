# AirBnB Project

Contributors: Anastasia Samoilova, Hillary Davis

## NYC Crime Data Instructions:
1. Go to the following link and download NYPD Complaint Data Current (Year To Date) as a CSV: https://data.cityofnewyork.us/Public-Safety/NYPD-Complaint-Data-Current-Year-To-Date-/5uac-w243
2. Rename the file “NYPD.csv”, upload it onto dataproc and place it into HDFS: `hdfs dfs -put NYPD.csv`
3. In HDFS, create a directory called: hillary_data, and move the csv into that directory
4. Similarly, upload all of the following files into dataproc and place them into HDFS:
    1. nyc_crime_ingest.scala
    2. nycCrime.scala
    3. crime_profiling.scala
5. Open the spark Scala shell: `spark-shell --deploy-mode client`
6. To begin - run nyc_crime_ingest.scala as follows
    1.  `:load nyc_crime_ingest.scala`
    2.  `NYPDComplaintsIngest.main(Array())`
7. Next - run nycCrime.scala as follows:
    1.  `:load nycCrime.scala`
    2.  `NYPDDataAnalysis.main(Array())`
8. Finally - run crime_profiling.scala as follows:
    1.  Get the name of the output file `hdfs dfs -ls hillary_data/output`, and change the name as such:
    `hadoop fs -mv hdfs://nyu-dataproc-m/user/<username>/hillary_data/output/<file name>.csv hdfs://nyu-dataproc-m/user/<username>/hillary_data/output/nyc_crime_clean.csv`
    2.  `:load crime_profiling.scala`
    3.  ` NYPDComplaintsCount.main(Array())`
9. Next, download the csv output into hillary_data/output:
    1. `hdfs dis -get hillary_data/output/<name of csv>`
    2. Select “download file” and enter the exact path of the csv
10. On your local machine make sure the csv file is in the same directory as the following files:
    1. crimeAnalysis.py
    2. geo_export_0055599d-8f10-45b4-afb9-9ce5823b628d.dbf
    3. geo_export_0055599d-8f10-45b4-afb9-9ce5823b628d.prj
    4. geo_export_0055599d-8f10-45b4-afb9-9ce5823b628d.shp
    5. geo_export_0055599d-8f10-45b4-afb9-9ce5823b628d.shx
11. Run crimeAnalysis.py to see a heat map of crime throughout various neighborhoods in NYC
12. Run crimeByNeighborhood.py to see a line graph of crime over time for all neighborhoods

## NYC AirBnB listings instructions:
1. Go to the following link to download the data "Detailed Listings Data" for New York, NY from archive from December 4th 2022: http://insideairbnb.com/get-the-data/
2. Go to as15026 hdfs, and the initial data is already located under `project/listings.csv`
3. Upload the following files into DataProc: 
    1. `nyc_listings_ingest.scala`
    2. `clean_data.scala`
    3. `join_data.scala`
4. Run those files using the following commands to clean data and get the joined dataset that has 2 additional rows from crime data -- non-felony crimes and felony crimes: 
    1. `spark-shell --deploy-mode client -i nyc_listings_ingest.scala`
    2. `spark-shell --deploy-mode client -i clean_data.scala`
    The output from this run is located in `project/output/clean_nyc_listings.csv`. Note that is has been manually renamed for the ease of use. 
    3. `spark-shell --deploy-mode client -i join_data.scala`
    The output from this run is located in `project/join_output/join_data.csv`. Note that is has been manually renamed for the ease of use.
5. Next, download the following files: 
    1. `joined_profiling.scala`
    2. `listings_profiling.scala`
6. Run the following commands to get the profiling information about both the joined dataset and the dataset with listings only:
    1. `spark-shell --deploy-mode client -i joined_profiling.scala`
    2. `spark-shell --deploy-mode client -i listings_profiling.scala`
    The output of these files comes as print out. 
7. Next, download the following file --  `joined_data_analytics.scala` -- it runs linear regression, computes correlation coefficients, and creates csv files with summary data. 
8. Run this file using this command: 
    1. `spark-shell --deploy-mode client -i joined_data_analytics.scala`
    The output can be found in `project/analytics` folder on as15026 hdfs, which contains 12 csv files with summary data (those files have been manually renamed for the ease of use as well). The results of linear regression and correlation coefficients are printed out after the run of the file. 
9. Next, move those files onto your local machine and make sure they are all in the same folder: 
    1. `graphs.py`
    2. `correlations.csv`
    3. `priceCrimes.csv`
    4. `ratingCrimes.csv`
    5. `ratingSuperhost.csv`
    6. `ratingReviews.csv`
10. Enter the following command: `python graphs.py`
    1. After the run of this file, 6 `png` files with graphs, tables, and heatmaps will appear in the same folder. They will be named: 
    `price_vs_crimes.png`, `rating_vs_crimes.png`, `rating_vs_superhost.png`, `rating_vs_reviews.png`, `rating_vs_price.png`, `heatmap_correlations.png`. 
