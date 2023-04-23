import pandas as pd
import matplotlib.pyplot as plt
from tabulate import tabulate

'''
This file created picatures of grapsh and tables.
'''

# Create a graph for Price vs Number of Crimes
df = pd.read_csv("priceCrimes.csv")
pr_cr = df[df["price_range"] < 500]
plt.plot(pr_cr["price_range"], pr_cr["mean_total_crimes"])
plt.xlabel("Price")
plt.ylabel("Mean total of crimes")
plt.title("Price vs Crimes")
plt.savefig("price_vs_crimes.png")
plt.clf()


# Create a graph for Rating vs Number of Crimes
df2 = pd.read_csv("ratingCrimes.csv")
ra_cr = df2[df2["rating_range"] > 3]
plt.plot(ra_cr["rating_range"], ra_cr["mean_total_crimes"])
plt.xlabel("Rating")
plt.ylabel("Mean total of crimes")
plt.title("Rating vs Crimes")
plt.savefig("rating_vs_crimes.png")
plt.clf()


# Create a table for Rating vs superhost 
df3 = pd.read_csv("ratingSuperhost.csv")
df3["mean_rating_range"] = df3["mean_rating_range"].round(2)
df3['superhost'] = df3['superhost'].replace({0.0: 'false', 1.0: 'true'})
table = tabulate(df3, headers='keys', tablefmt='grid')
fig, ax = plt.subplots()
ax.axis('off')
ax.axis('tight')
table_plot = ax.table(cellText=df3.values, colLabels=df3.columns, loc='center')
plt.savefig("rating_vs_superhost.png")
plt.clf()


# Create a graph for Rating vs Number of reviews 
ra_re = pd.read_csv("ratingReviews.csv")
plt.plot(ra_re["rating_range"], ra_re["mean_number_of_reviews"])
plt.xlabel("Rating")
plt.ylabel("Mean numer of reviews")
plt.title("Rating vs Number of reviews")
plt.savefig("rating_vs_reviews.png")
plt.clf()


# Create a graph for Rating vs Price 
df4 = pd.read_csv("ratingPrice.csv")
ra_pr = df4[df4["rating_range"] > 3]
plt.plot(ra_pr["rating_range"], ra_pr["mean_price_range"])
plt.xlabel("Rating")
plt.ylabel("Mean price range")
plt.title("Rating vs Mean price")
plt.savefig("rating_vs_price.png")
plt.clf()


# Create a heatmap for correlations 
df5 = pd.read_csv('correlations.csv', header=None)
df5.columns = ['host_total_listings_count', 'number_of_reviews', 'price_range', 'rating_range', 'superhost', 'host_verified', 'neighbourhood_match', 'non_felony_crimes', 'felony_crimes']
df5.index = ['host_total_listings_count', 'number_of_reviews', 'price_range', 'rating_range', 'superhost', 'host_verified', 'neighbourhood_match', 'non_felony_crimes', 'felony_crimes']
df5 = df5.round(2)
plt.imshow(df5, cmap='coolwarm')

for i in range(len(df5)):
    for j in range(len(df5)):
        plt.text(j, i, df5.iloc[i, j], ha='center', va='center')

plt.colorbar()
plt.tight_layout()
plt.savefig('heatmap_correlations.png')

print("All graphs and tables have been successfully created and saved!")
