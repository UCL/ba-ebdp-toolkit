# %%
import geopandas as gpd

# %%
# Load GeoDataFrame from a GeoParquet file
file_path = "/Users/gareth/dev/other/ba-ebdp-toolkit/temp/eu_segment_metrics.parquet"
gdf = gpd.read_parquet(file_path)

# Display the first few rows of the GeoDataFrame
print(gdf.head())
