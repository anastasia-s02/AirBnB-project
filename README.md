# AirBnB-project

NYC Crime Data Instructions:
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
    1.  `:load crime_profiling.scala`
    2.  ` NYPDComplaintsCount.main(Array())`
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