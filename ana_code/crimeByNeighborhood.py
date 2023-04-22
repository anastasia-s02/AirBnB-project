import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt

df = pd.read_csv('nyc_crime_clean.csv', parse_dates=['CMPLNT_FR_DT'])
print("Crime data loaded successfully")

# Create GeoDataFrame from latitude and longitude
gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.Longitude, df.Latitude))
gdf.crs = "EPSG:4326"
print("GeoDataFrame created successfully")

# Load neighborhood boundaries from: https://data.cityofnewyork.us/City-Government/2010-Neighborhood-Tabulation-Areas-NTAs-/cpf4-rkhq
nyc_neighborhoods = gpd.read_file('geo_export_0055599d-8f10-45b4-afb9-9ce5823b628d.shp')
nyc_neighborhoods = nyc_neighborhoods.to_crs(gdf.crs)
print("Neighborhood boundaries loaded successfully")

# Spatial join between crime data and neighborhood boundaries
crime_by_neighborhood = gpd.sjoin(gdf, nyc_neighborhoods, op='within')
print("Spatial join performed successfully")

# Calculate crime rate for each neighborhood and date
crime_by_neighborhood['date'] = pd.to_datetime(crime_by_neighborhood['CMPLNT_FR_DT'])
crime_by_neighborhood['year_month'] = crime_by_neighborhood['date'].dt.strftime('%Y-%m')
crime_by_neighborhood_grouped = crime_by_neighborhood.groupby(['ntaname', 'year_month'])['LAW_CAT_CD'].count().reset_index()
crime_by_neighborhood_grouped.rename(columns={'LAW_CAT_CD': 'crime_count'}, inplace=True)

# Visualize crime trends for each neighborhood
fig, ax = plt.subplots(figsize=(15, 8))
for neighborhood in nyc_neighborhoods['ntaname']:
    data = crime_by_neighborhood_grouped[crime_by_neighborhood_grouped['ntaname'] == neighborhood]
    if not data.empty:
        ax.plot(data['year_month'], data['crime_count'], label=neighborhood)
ax.set_xlabel('Year-Month')
ax.set_ylabel('Crime Count')
ax.set_title('Crime Trends by Neighborhood')
ax.legend(loc='center left', bbox_to_anchor=(1.0, 0.5))
plt.show()