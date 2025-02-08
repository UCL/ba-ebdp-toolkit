# %%
import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns

sns.set_theme(style="ticks")

# %%
cols = [
    "cc_beta_2000",
    "cc_beta_5000",
    "cc_beta_10000",
    "cc_betweenness_2000",
    "cc_betweenness_5000",
    "cc_betweenness_10000",
    "cc_green_nearest_max_1500",
    "cc_trees_nearest_max_1500",
    "cc_hill_q0_500_nw",
    "cc_restaurant_500_nw",
    "cc_bar_500_nw",
    "cc_cafe_500_nw",
    "cc_accommodation_500_nw",
    "cc_automotive_500_nw",
    "cc_arts_and_entertainment_500_nw",
    "cc_attractions_and_activities_500_nw",
    "cc_active_life_500_nw",
    "cc_beauty_and_spa_500_nw",
    "cc_education_500_nw",
    "cc_financial_service_500_nw",
    "cc_private_establishments_and_corporates_500_nw",
    "cc_retail_500_nw",
    "cc_health_and_medical_500_nw",
    "cc_pets_500_nw",
    "cc_business_to_business_500_nw",
    "cc_public_service_and_government_500_nw",
    "cc_religious_organization_500_nw",
    "cc_real_estate_500_nw",
    "cc_travel_500_nw",
    "cc_home_service_500_nw",
    "cc_professional_services_500_nw",
    "cc_street_furn_500_nw",
    "cc_parking_500_nw",
    "cc_transport_500_nw",
    "cc_perimeter_mean_1500_wt",
    "cc_compactness_mean_1500_wt",
    "cc_orientation_mean_1500_wt",
    "cc_volume_mean_1500_wt",
    "cc_floor_area_ratio_mean_1500_wt",
    "cc_form_factor_mean_1500_wt",
    "cc_corners_mean_1500_wt",
    "cc_shape_index_mean_1500_wt",
    "cc_fractal_dimension_mean_1500_wt",
    "cc_block_area_mean_1500_wt",
    "cc_block_perimeter_mean_1500_wt",
    "cc_block_compactness_mean_1500_wt",
    "cc_block_orientation_mean_1500_wt",
    "cc_block_covered_ratio_mean_1500_wt",
]
stats_cols = ["m", "f", "y_lt15", "y_1564", "y_ge65", "emp", "nat", "eu_oth", "oth", "same", "chg_in", "chg_out"]

# %%
# Load GeoDataFrame from a GeoParquet file
file_path = "temp/t2e_metrics.parquet"
gdf = gpd.read_parquet(
    file_path,
    columns=[
        "fid",
        "x",
        "y",
        "geom",
        "t",
    ]
    + stats_cols
    + cols,
    bbox=None,
)

# %%
MAX_ROWS = 3000000  # 4m too much for 16GB RAM

# Shuffle and subsample
print(len(gdf))
if len(gdf) > MAX_ROWS:  # noqa: SIM108
    gdf = gdf.sample(n=MAX_ROWS, random_state=42)  # Randomly sample 50,000 rows
else:
    gdf = gdf.sample(frac=1, random_state=42)  # Just shuffle if under 50,000

# Reset index
gdf = gdf.reset_index(drop=True)

# %%
gdf.head()

# %%
list(gdf.columns)
# %%
# clip negative interpolation until rerun with linear
for col in stats_cols:
    gdf[col] = np.clip(gdf[col], 0, None)  # Ensures values are >= 0

# Optionally, replace NaN values back with 0 if needed
gdf.fillna(0, inplace=True)

# Avoid division by zero by replacing zeros in 'p' with NaN
gdf["t"] = gdf["t"].replace(0, 1)

# Normalize each column by 'p'
for col in stats_cols:
    gdf[f"{col}_perc"] = (gdf[col] / gdf["t"]) * 100

# Display the first few rows to check
print(gdf.head())


# %%
explore_cols = [
    "t",
    "m_perc",
    "f_perc",
    "y_lt15_perc",
    "y_1564_perc",
    "y_ge65_perc",
    # "emp_perc",
    "nat_perc",
    "eu_oth_perc",
    "oth_perc",
    "same_perc",
    "chg_in_perc",
    "chg_out_perc",
    "cc_green_nearest_max_1500",
    "cc_trees_nearest_max_1500",
    "cc_bar_1500_nw",
    "cc_restaurant_1500_nw",
    "cc_cafe_1500_nw",
    "cc_education_1500_nw",
    "cc_retail_1500_nw",
    "cc_hill_q0_1500_nw",
]

# %%
for col in explore_cols:
    print(gdf[[col]].describe().loc["mean"])

# %%

for col in explore_cols:
    # Compute statistics
    mean_val = gdf[col].mean()
    median_val = gdf[col].median()
    std_val = gdf[col].std()
    percentile_98 = gdf[col].quantile(0.98)
    col_min = gdf[col].min()
    col_max = gdf[col].max()

    plt.figure(figsize=(8, 6))

    # Ensure bins are at least 1 unit wide for integer-type data
    binwidth = max(1, (percentile_98 - col_min) // 30)

    sns.histplot(gdf[col], binwidth=binwidth)
    if "perc" in col:
        plt.xlim(0, 100)
    else:
        plt.xlim(col_min, percentile_98)

    # Add vertical lines for summary statistics
    plt.axvline(mean_val, color="red", linestyle="--", label=f"Mean: {mean_val:.2f}")
    plt.axvline(median_val, color="blue", linestyle="-.", label=f"Median: {median_val:.2f}")

    if std_val > 0:  # Only add std dev lines if std deviation is nonzero
        plt.axvline(mean_val + std_val, color="green", linestyle=":", label=f"Std Dev: {std_val:.2f}")
        plt.axvline(mean_val - std_val, color="green", linestyle=":", label="_nolegend_")  # Avoid duplicate legend

    # Add legend and labels
    plt.legend()
    plt.title(f"Histogram of {col}")
    plt.xlabel(col)
    plt.ylabel("Frequency")

    # Display the plot
    plt.show()

# %%

var_cols = [
    "t",
    "m_perc",
    "f_perc",
    "y_lt15_perc",
    "y_1564_perc",
    "y_ge65_perc",
    "emp_perc",
    "nat_perc",
    "eu_oth_perc",
    "oth_perc",
    "same_perc",
    "chg_in_perc",
    "chg_out_perc",
] + cols


# %%
def visualise_corr(gdf):
    plt.figure(figsize=(50, 40), dpi=150)
    # Compute correlation between cluster labels and original features
    corr_matrix = gdf.corr()
    # Plot the correlation matrix
    plt.figure(figsize=(50, 40), dpi=150)
    sns.heatmap(corr_matrix, annot=True, cmap="coolwarm", fmt=".2f", vmin=-1, vmax=1, linewidths=0.5)
    # Title and display
    plt.title("Correlation Matrix")
    plt.show()


visualise_corr(gdf[var_cols])

# %%
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

# Standardize the numeric columns
data = gdf[var_cols]
scaler = StandardScaler()
data = scaler.fit_transform(data)

# %%
# Apply KMeans clustering (e.g., with 10 clusters)
kmeans = KMeans(n_clusters=10, random_state=42)
gdf["kmeans"] = kmeans.fit_predict(data)

cluster_1_data = data[gdf["kmeans"] == 2]  # Filter data for cluster 1
visualise_corr(pd.DataFrame(cluster_1_data, columns=var_cols))

# %%
