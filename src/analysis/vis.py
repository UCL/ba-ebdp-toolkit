""" """

# %%
import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
from IPython.display import display
from scipy.spatial import KDTree

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
    "cc_hill_q0_1500_wt",
    "cc_restaurant_500_nw",
    "cc_restaurant_1500_wt",
    "cc_bar_500_nw",
    "cc_bar_1500_wt",
    "cc_cafe_500_nw",
    "cc_cafe_1500_wt",
    "cc_accommodation_500_nw",
    "cc_accommodation_1500_wt",
    "cc_automotive_500_nw",
    "cc_automotive_1500_wt",
    "cc_arts_and_entertainment_500_nw",
    "cc_arts_and_entertainment_1500_wt",
    "cc_attractions_and_activities_500_nw",
    "cc_attractions_and_activities_1500_wt",
    "cc_active_life_500_nw",
    "cc_active_life_1500_wt",
    "cc_beauty_and_spa_500_nw",
    "cc_beauty_and_spa_1500_wt",
    "cc_education_500_nw",
    "cc_education_1500_wt",
    "cc_financial_service_500_nw",
    "cc_financial_service_1500_wt",
    "cc_private_establishments_and_corporates_500_nw",
    "cc_private_establishments_and_corporates_1500_wt",
    "cc_retail_500_nw",
    "cc_retail_1500_wt",
    "cc_health_and_medical_500_nw",
    "cc_health_and_medical_1500_wt",
    "cc_pets_500_nw",
    "cc_pets_1500_wt",
    "cc_business_to_business_500_nw",
    "cc_business_to_business_1500_wt",
    "cc_public_service_and_government_500_nw",
    "cc_public_service_and_government_1500_wt",
    "cc_religious_organization_500_nw",
    "cc_religious_organization_1500_wt",
    "cc_real_estate_500_nw",
    "cc_real_estate_1500_wt",
    "cc_travel_500_nw",
    "cc_travel_1500_wt",
    "cc_home_service_500_nw",
    "cc_home_service_1500_wt",
    "cc_professional_services_500_nw",
    "cc_professional_services_1500_wt",
    "cc_street_furn_500_nw",
    "cc_street_furn_1500_wt",
    "cc_parking_500_nw",
    "cc_parking_1500_wt",
    "cc_transport_500_nw",
    "cc_transport_1500_wt",
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
# if len(gdf) > MAX_ROWS:  # noqa: SIM108
#    gdf = gdf.sample(n=MAX_ROWS, random_state=42)  # Randomly sample 50,000 rows
# else:
#    gdf = gdf.sample(frac=1, random_state=42)  # Just shuffle if under 50,000

# %%
# Reset index
gdf = gdf.reset_index(drop=True)

# %%
print(len(gdf))
gdf.head()

# %%
list(gdf.columns)

# %%
# replace NaN values with 0
gdf.loc[:, "t"] = gdf["t"].fillna(0)
gdf.loc[:, stats_cols] = gdf[stats_cols].fillna(0)

# clip for negative interpolation
gdf.loc[:, "t"] = gdf["t"].clip(0, None)

# Clip stats values to valid range [t, t] row by row
for col in stats_cols:
    # Check for values below t
    mask_min = gdf[col] < 0
    if mask_min.any():
        print(f"Clipping {col} to 0 for {mask_min.sum()} rows")
        gdf.loc[mask_min, col] = 0

    # Check for values above t
    mask_max = gdf[col] > gdf["t"]
    if mask_max.any():
        print(f"Capping {col} to population max for {mask_max.sum()} rows")
        gdf.loc[mask_max, col] = gdf.loc[mask_max, "t"]

# Avoid division by zero by replacing zeros in population counts with 1
gdf.loc[:, "t"] = gdf["t"].clip(1, None)

# Normalize each stats column by population
for col in stats_cols:
    gdf[f"{col}_perc"] = (gdf[col] / gdf["t"]) * 100

# %%
max_cols = [c for c in cols if "max_1500" in c]
gdf.loc[:, max_cols] = gdf[max_cols].fillna(1500)


# %%
def impute_spatial_multi(gdf, target_cols, n_neighbors=3):
    """Impute missing values for multiple columns based on spatial proximity using KDTree."""
    coords = gdf[["x", "y"]].values
    for col in target_cols:
        print(f"Imputing {col}")
        values = gdf[col].values
        nan_mask = np.isnan(values)
        # Use only known values to build KDTree
        known_coords = coords[~nan_mask]
        known_values = values[~nan_mask]
        if known_coords.shape[0] == 0:
            print(f"Skipping {col} (no known values)")
            continue
        # Build KDTree on known data only
        tree = KDTree(known_coords)
        # Query nearest neighbors for NaN values
        distances, indices = tree.query(coords[nan_mask], k=n_neighbors)
        # Compute weighted average of nearest neighbors
        weights = np.where(distances > 0, 1 / distances, 0)  # Avoid division by zero
        weights /= weights.sum(axis=1, keepdims=True)  # Normalize weights
        imputed_values = np.sum(weights * known_values[indices], axis=1)
        # Assign imputed values back to DataFrame
        gdf.loc[nan_mask, col] = imputed_values

    return gdf


# Example usage:
mean_cols = [c for c in gdf.columns if "_mean_" in c]

print("NaN values before imputation:")
display(gdf[mean_cols].isna().sum())

gdf = impute_spatial_multi(gdf, mean_cols)

print("NaN values after imputation:")
display(gdf[mean_cols].isna().sum())


# %%
nan_columns = gdf.columns[gdf.isna().any()].tolist()
print("Columns with NaN values:")
print(nan_columns)

# %%
explore_cols = [
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
    "cc_green_nearest_max_1500",
    "cc_trees_nearest_max_1500",
    "cc_bar_1500_wt",
    "cc_restaurant_1500_wt",
    "cc_cafe_1500_wt",
    "cc_education_1500_wt",
    "cc_retail_1500_wt",
    "cc_hill_q0_1500_wt",
]

# %%
for col in explore_cols:
    desc = gdf[[col]].describe()
    display(desc.round(2))

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

    if "perc" in col:
        binwidth = 1
        plt.xlim(0, 100)
    elif "_wt" in col:
        binwidth = 0.1
        plt.xlim(col_min, percentile_98)
    else:
        binwidth = max(0.1, (percentile_98 - col_min) // 30)
        plt.xlim(col_min, percentile_98)

    sns.histplot(gdf[col], binwidth=binwidth)

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
] + [c for c in cols if "_nw" not in c]


# %%
def visualise_corr(data: np.ndarray, labels, title, save_path=None):
    """Visualize and save the correlation matrix."""
    # Calculate correlation matrix from numpy array
    corr_matrix = np.corrcoef(data.T)
    plt.figure(figsize=(50, 40), dpi=150)
    # Plot the correlation matrix with column names
    sns.heatmap(
        corr_matrix,
        annot=True,
        cmap="coolwarm",
        fmt=".2f",
        vmin=-1,
        vmax=1,
        linewidths=0.5,
        xticklabels=labels,
        yticklabels=labels,
    )
    plt.title(f"Correlation Matrix {title}")

    # Save the plot if save_path is provided
    if save_path:
        plt.savefig(save_path, bbox_inches="tight")

    # Display the plot
    plt.show()


# %%
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA
from sklearn.preprocessing import QuantileTransformer

# %%
# Standardize the numeric columns
data = gdf[var_cols]
scaler = QuantileTransformer(output_distribution="uniform", n_quantiles=1000)
data = scaler.fit_transform(data)

# %%
visualise_corr(data, var_cols, "Dataset", save_path="temp/correlation_matrix.png")

# %%
pca = PCA(n_components=0.95)
pca_data = pca.fit_transform(data)
for idx in range(1, 11):
    gdf[f"pca_{idx}"] = pca_data[:, idx]

# %%
cluster_model = KMeans(n_clusters=25)
gdf["cluster_pca"] = cluster_model.fit_predict(pca_data)
gdf["cluster"] = cluster_model.fit_predict(data)

# %%
# for cluster_key in gdf["cluster"].unique():
#     cluster_data = data[gdf["cluster"] == cluster_key]
#     visualise_corr(
#         cluster_data, var_cols, f"Cluster {cluster_key} specific correlations", f"temp/cluster_{cluster_key}_corrs.png"
#     )

# %%
gdf[["pca_1", "pca_2", "pca_3", "cluster", "cluster_pca", "geom"]].to_file("temp/t2e_metrics_clusters.gpkg")

# %%
