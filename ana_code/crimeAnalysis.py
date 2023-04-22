import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt

# Load crime data
df = pd.read_csv('nyc_crime_clean.csv')
print("Crime data loaded successfully")

# Create GeoDataFrame from latitude and longitude
gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.Longitude, df.Latitude))
gdf.crs = "EPSG:4326"
print("GeoDataFrame created successfully")

# Load neighborhood boundaries from: https://data.cityofnewyork.us/City-Government/2010-Neighborhood-Tabulation-Areas-NTAs-/cpf4-rkhq
nyc_neighborhoods = gpd.read_file('geo_export_0055599d-8f10-45b4-afb9-9ce5823b628d.shp')
nyc_neighborhoods = nyc_neighborhoods.to_crs(gdf.crs)
print("Neighborhood boundaries loaded successfully")

# Spatial join between crime data and neigh boundaries
crime_by_neighborhood = gpd.sjoin(gdf, nyc_neighborhoods, op='within')
print("Spatial join between crime data and neighborhood boundaries successful")

# Calculate crime rate for each neigh
crime_rate = crime_by_neighborhood.groupby('ntaname')['LAW_CAT_CD'].count()
print("Crime rate calculated successfully")

# Visualize crime rates on a map
nyc_neighborhoods = nyc_neighborhoods.merge(crime_rate, left_on='ntaname', right_index=True)
nyc_neighborhoods.plot(column='LAW_CAT_CD', cmap='Reds', legend=True)
plt.show()
print("Map displayed successfully")

