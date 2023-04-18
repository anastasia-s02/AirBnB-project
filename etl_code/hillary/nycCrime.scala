import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{mean, expr}


object NYPDDataAnalysis {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("NYPD Complaint Data")
      .getOrCreate()

    // Read data from CSV file and drop null values
    var data: DataFrame = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("NYPD.csv")
      .drop("CMPLNT_FR_TM", "HADEVELOP", "HOUSING_PSA", "PARKS_NM", 
      "PATROL_BORO", "STATION_NAME", "TRANSIT_DISTRICT", "SUSP_AGE_GROUP", 
      "Latitude", "Longitude", "New Georeferenced Column", "Community Districts", 
      "City Council Districts", "CMPLNT_NUM", "ADDR_PCT_CD", "CMPLNT_TO_TM", 
      "HADEVELOPT", "JURIS_DESC", "RPT_DT", "X_COORD_CD", "Y_COORD_CD", "Borough Boundaries")
      .na.drop()
        // Filter rows with CMPLNT_TO_DT after or equal to 01/01/2017
    data = data.filter(to_date(col("CMPLNT_FR_DT"), "MM/dd/yyyy") >= to_date(lit("01/01/2017"), "MM/dd/yyyy"))
    // Rename columns with spaces to have an underscore (TEXT FORMATING - CODE CLEANING PT 1)
    data = data.toDF(data.columns.map(_.replaceAll(" ", "_")): _*)
    // Find distinct values in each column and the count of text data
    data.columns.foreach(column => {
        val distinctValues = data.select(column).distinct()
        val countDistinctValues = distinctValues.count()
        println(s"Distinct values in column $column: $countDistinctValues")
        distinctValues.show(false)
    })

    // Create a binary column based on the condition of another column.

    val dataWithBinaryCol = data.withColumn("binary_col", when(col("LAW_CAT_CD").isin("FELONY"), 1).otherwise(0))
    def mapNeighborhood(pct: Int): String = pct match {
            case 1 => "Little Italy, Tribeca, and Lower Manhattan"
            case 5 => "Lower East Side and Little Italy"
            case 6 => "Greenwich Village"
            case 7 => "Lower East Side and FDR Drive"
            case 9 => "Lower East Side, Tompkins Square, and East Village"
            case 10 => "Chelsea and Lincoln Tunnel"
            case 13 => "Stuyvesant Town, Tudor City, Turtle Bay, Peter Cooper Village, Murray Hill, and Gramercy Park"
            case 17 => "Turtle Bay, Tudor City, Murray Hill, Gramercy Park, Peter Cooper Village, and Stuyvesant Town"
            case 19 => "Upper East Side, Yorkville, Lenox Hill, and Roosevelt Island"
            case 20 => "Lincoln Square and Columbus Avenue South"
            case 23 => "East Harlem"
            case 24 => "Upper West Side and Manhattan Valley"
            case 25 => "East Harlem"
            case 26 => "Morningside Heights, Hamilton Heights, West Harlem, and Manhattanville"
            case 28 => "Central Harlem"
            case 30 => "Central Harlem"
            case 32 => "Central Harlem"
            case 33 => "Washington Heights and Inwood"
            case 34 => "Washington Heights and Inwood"
            case 60 => "Coney Island and Brighton Beach"
            case 61 => "Sheepshead Bay and Manhattan Beach"
            case 62 => "Bath Beach, Gravesend, and Bensonhurst"
            case 63 => "Bergen Beach and Mill Basin"
            case 66 => "Borough Park"
            case 67 => "Remsen Village, Farragut, and Flatbush"
            case 68 => "Bayridge"
            case 69 => "Flatlands and Canarsie"
            case 70 => "Flatbush and Midwood"
            case 71 => "Crown Heights"
            case 72 => "Sunset Park and Windsor Terrace"
            case 73 => "Brownsville"
            case 75 => "East New York and Cypress Hill"
            case 76 => "Red Hook and Carroll Gardens"
            case 77 => "Crown Heights"
            case 78 => "Park Slope"
            case 79 => "Bedford-Stuyvesant"
            case 81 => "Bedford-Stuyvesant"
            case 83 => "Bushwick and Ridgewood"
            case 84 => "Brooklyn Heights and Fulton Mall"
            case 88 => "Clinton Hill"
            case 90 => "Flushing Avenue and Williamsburg"
            case 94 => "Greenpoint and Williamsburg"
            case 40 => "Melrose, Mott Haven, and Port Morris"
            case 41 => "Hunts Point, Longwood"
            case 42 => "Morrisania, Crotona Park East"
            case 43 => "Soundview, Parkchester"
            case 44 => "Highbridge, Concourse Village"
            case 45 => "Throgs Neck, Co-op City, Pelham Bay"
            case 46 => "University Heights, Fordham, Mt. Hope"
            case 47 => "Wakefield, Williamsbridge"
            case 48 => "East Tremont, Belmont"
            case 49 => "Pelham Parkway, Morris Park, Laconia"
            case 50 => "Riverdale, Kingsbridge, Marble Hill"
            case 52 => "Bedford Park, Norwood, Fordham"
            case 100 => "Breezy Point, Belle Harbor, Rockaway Park, Rockaway" 
            case 101 => "Far Rockaway"
            case 102 => "Richmond Hill, Woodhaven, Ozone Park, Kew Gardens" 
            case 103 => "Jamaica, South Jamaica, Hollis" 
            case 104 => "Ridgewood, Glendale, Middle Village, Maspeth, Liberty Park" 
            case 105 => "Queens Village, Glen Oaks, New Hyde Park, Bellerose, Cambria Heights, Laurelton, Rosedale, Floral Park" 
            case 106 => "Howard Beach, South Ozone Park, Richmond Hill, Tudor Village, Lindenwood"
            case 107 => "Fresh Meadows, Cunningham Heights, Hilltop Village, Pomonak Houses, Jamaica Estates, Holliswood, Flushing South, Utopia, Kew Gardens Hills, Briarwood" 
            case 108 => "Long Island City, Woodside, Sunnyside" 
            case 109 => "Flushing, Bay Terrace, College Point, Whitestone, Malba, Beechhurst, Queensboro Hill, Willets Point" 
            case 110 => "Elmhurst, Corona, Roosevelt Avenue, Lefrak City, Queens Ctr Mall, Flushing Meadows, Corona Park" 
            case 111 => "Bayside, Douglaston, Little Neck, Auburndale, East Flushing, Oakland Gardens, Hollis Hills" 
            case 112 => "Forest Hills, Rego Park"
            case 113 => "St. Albans, Springfield Gardens, South Ozone Pk, Baisley Pk, Rochdale Village, South Jamaica" 
            case 114 => "Astoria, Old Astoria, Long Island City, Queensbridge, Ditmars, Ravenswood, Steinway, Garden Bay, Woodside" 
            case 115 => "Jackson Heights, East Elmhurst, North Corona, LaGuardia Airport"
            case 120 => "Arlington, Castleton Corners, Clifton, Concord, Elm Park, Fort Wadsworth, Graniteville, Grymes Hill, Livingston, Mariners Harbor, Meiers Corners, New Brighton, Port Ivory, Port Richmond, Randall Manor, Rosebank, St. George, Shore Acres, Silver Lake, Stapleton" 
            case 122 => "Arrochar, Bloomfield, Bulls Heads, Chelsea, Dongan Hills, Egbertville, Emerson Hill, Grant City, Grasmere, Midland Beach, New Dorp, New Springville, Oakwood, Ocean Breeze, Old Town, South Beach, Todt Hill, Travis" 
            case 123 => "Annadale, Arden Heights, Bay Terrace, Charleston, Eltingville, Great Kills, Greenridge, Huguenot, Pleasant Plains, Prince's Bay, Richmond Valley, Rossville, Tottenville, Woodrow"
            case _ => "Unknown"
        }
      val mapNeighborhoodUDF = udf(mapNeighborhood _)
      var dfWithNeighborhood = dataWithBinaryCol.withColumn("Neighborhood", mapNeighborhoodUDF(col("Police_Precincts")))
      dfWithNeighborhood = dfWithNeighborhood.filter(col("Neighborhood") =!= "Unknown")
      dfWithNeighborhood.show()
      dfWithNeighborhood.coalesce(1).write.format("csv").option("header","true").mode("overwrite").save("hdfs://nyu-dataproc-m/user/hcd258_nyu_edu/hillary_data/output")
    spark.stop()
  }
}

