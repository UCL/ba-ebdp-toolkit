# %%

from pathlib import Path

import fiona
import matplotlib.pyplot as plt
import networkx as nx
from cityseer import rustalgos
from cityseer.metrics import networks
from cityseer.tools import graphs, io, plot
from shapely import geometry


# %%
def nx_from_barcelona_gpkg(
    data_path: Path,
) -> nx.MultiGraph:
    """ """
    print("Loading Barcelona network")
    # create a networkX multigraph
    g_multi = nx.MultiGraph()
    # load
    nodes = {}
    edges = []
    # load the edges
    with fiona.open(data_path) as edges_src:
        for edge_data in edges_src.values():
            edge_props: dict = dict(edge_data.properties)
            edge_geoms: geometry.LineString | geometry.MultiLineString = geometry.shape(edge_data.geometry)
            if isinstance(edge_geoms, geometry.LineString):
                edge_geoms = geometry.MultiLineString([edge_geoms])
            for edge_geom in edge_geoms.geoms:
                # start node
                nd_a = edge_geom.coords[0]
                nd_a_key = f"{nd_a[0]}-{nd_a[1]}"
                nodes[nd_a_key] = nd_a
                # end node
                nd_b = edge_geom.coords[-1]
                nd_b_key = f"{nd_b[0]}-{nd_b[1]}"
                nodes[nd_b_key] = nd_b
                # edge
                edges.append((edge_props, edge_geom, nd_a_key, nd_b_key))
    for nd_key, node in nodes.items():
        g_multi.add_node(nd_key, x=node[0], y=node[1])
    for _edge_props, edge_geom, nd_a_key, nd_b_key in edges:
        g_multi.add_edge(nd_a_key, nd_b_key, geom=edge_geom)

    print(f"Nodes: {g_multi.number_of_nodes()}")
    print(f"Edges: {g_multi.number_of_edges()}")

    return g_multi


# %%
repo_path = Path("../..")
barc_netw_path = Path(repo_path / "case_data/barcelona_road_network.gpkg")
print("data path:", barc_netw_path)
print("path exists:", barc_netw_path.exists())
distances = [500, 2000]
barc_netw = nx_from_barcelona_gpkg(barc_netw_path)
barc_netw = graphs.nx_remove_filler_nodes(barc_netw)
barc_netw = graphs.nx_remove_dangling_nodes(barc_netw, despine=15)
# prepare data structures
nodes_gdf, _edges_gdf, network_structure = io.network_structure_from_nx(barc_netw, crs=3035)
# compute centralities
# if computing wider area centralities, e.g. 20km, then use less decomposition to speed up the computation
nodes_gdf = networks.node_centrality_shortest(
    network_structure=network_structure,
    nodes_gdf=nodes_gdf,
    distances=distances,
)
# compute simplest path centrality
nodes_gdf = networks.node_centrality_simplest(
    network_structure=network_structure,
    nodes_gdf=nodes_gdf,
    distances=distances,
)

# %%
# prepare OSM
barc_osm_geom_path = Path(repo_path / "case_data/barcelona_network_extent.gpkg")
print("data path:", barc_osm_geom_path)
print("path exists:", barc_osm_geom_path.exists())

with fiona.open(barc_osm_geom_path) as src:
    for feature in src:
        extent_multi_geom = geometry.shape(feature.geometry)
extent_geom = extent_multi_geom.geoms[0].simplify(50)

# %%
# minimal cleaning
barc_netw_osm = io.osm_graph_from_poly(
    poly_geom=extent_geom,
    poly_crs_code=3035,
    to_crs_code=3035,
    simplify=False,
)
barc_netw_osm = graphs.nx_remove_dangling_nodes(barc_netw_osm, despine=15)
# prepare data structures
nodes_gdf_osm, _edges_gdf_osm, network_structure_osm = io.network_structure_from_nx(barc_netw_osm, crs=3035)
# compute centralities
# if computing wider area centralities, e.g. 20km, then use less decomposition to speed up the computation
nodes_gdf_osm = networks.node_centrality_shortest(
    network_structure=network_structure_osm,
    nodes_gdf=nodes_gdf_osm,
    distances=distances,
)
# compute simplest path centrality
nodes_gdf_osm = networks.node_centrality_simplest(
    network_structure=network_structure_osm,
    nodes_gdf=nodes_gdf_osm,
    distances=distances,
)

# %%
# automatic cleaning
barc_netw_osm_cleaned = io.osm_graph_from_poly(
    poly_geom=extent_geom,
    poly_crs_code=3035,
    to_crs_code=3035,
    crawl_consolidate_dist=15,
    parallel_consolidate_dist=20,
    contains_buffer_dist=100,
    simplify=True,
    iron_edges=True,
)
# prepare data structures
nodes_gdf_osm_cleaned, edges_gdf_osm_cleaned, network_structure_osm_cleaned = io.network_structure_from_nx(
    barc_netw_osm_cleaned, crs=3035
)
# compute centralities
# if computing wider area centralities, e.g. 20km, then use less decomposition to speed up the computation
nodes_gdf_osm_cleaned = networks.node_centrality_shortest(
    network_structure=network_structure_osm_cleaned,
    nodes_gdf=nodes_gdf_osm_cleaned,
    distances=distances,
)
# compute simplest path centrality
nodes_gdf_osm_cleaned = networks.node_centrality_simplest(
    network_structure=network_structure_osm_cleaned,
    nodes_gdf=nodes_gdf_osm_cleaned,
    distances=distances,
)
# %%
# create temp directory if necessary
(repo_path / Path("temp")).mkdir(parents=False, exist_ok=True)
# write outputs
nodes_gdf_osm_cleaned.to_file(repo_path / Path("temp/barcelona_cleaned_nodes.gpkg"))
edges_gdf_osm_cleaned.to_file(repo_path / Path("temp/barcelona_cleaned_edges.gpkg"))

# %%
easting, northing = 3664195.3, 2066277.3
plot_buffer = 1500
centroid = geometry.Point(easting, northing)
plot_bbox: tuple[float, float, float, float] = centroid.buffer(plot_buffer).bounds
bg_colour = "#111"
font_colour = "lightgrey"
font_size = 7
s, n = plot_bbox[1], plot_bbox[3]
e, w = plot_bbox[0] + 750, plot_bbox[2] - 750
print(plot_bbox, e, w)
fig, axes = plt.subplots(1, 3, figsize=(8, 6), dpi=200, facecolor=bg_colour)
fig.suptitle("Source network comparisons", color=font_colour)
plot.plot_nx(
    barc_netw,
    node_size=1,
    edge_colour="#666",
    edge_width=0.5,
    plot_geoms=True,
    x_lim=(e, w),
    y_lim=(s, n),
    ax=axes[0],
)
axes[0].set_title("Official network graph", fontsize=font_size, color=font_colour)
axes[0].set_aspect("equal")
plot.plot_nx(
    barc_netw_osm,
    node_size=1,
    edge_colour="#666",
    edge_width=0.5,
    plot_geoms=True,
    x_lim=(e, w),
    y_lim=(s, n),
    ax=axes[1],
)
axes[1].set_title("OSM network", fontsize=font_size, color=font_colour)
axes[1].set_aspect("equal")
plot.plot_nx(
    barc_netw_osm_cleaned,
    node_size=1,
    edge_colour="#666",
    edge_width=0.5,
    plot_geoms=True,
    x_lim=(e, w),
    y_lim=(s, n),
    ax=axes[2],
)
axes[2].set_title("OSM network algo. cleaned", fontsize=font_size, color=font_colour)
axes[2].set_aspect("equal")
plt.tight_layout()
plt.gcf().set_facecolor(bg_colour)
plt.show()

# %%
# weighted
barc_netw_osm_wt = graphs.nx_weight_by_dissolved_edges(barc_netw_osm, dissolve_distance=20, max_ang_diff=45)
nodes_gdf_osm_wt, _edges_gdf_osm_wt, network_structure_osm_wt = io.network_structure_from_nx(barc_netw_osm_wt, crs=3035)
# compute centralities
# if computing wider area centralities, e.g. 20km, then use less decomposition to speed up the computation
nodes_gdf_osm_wt = networks.node_centrality_shortest(
    network_structure=network_structure_osm_wt,
    nodes_gdf=nodes_gdf_osm_wt,
    distances=distances,
    jitter_scale=20,
)
# compute simplest path centrality
nodes_gdf_osm_wt = networks.node_centrality_simplest(
    network_structure=network_structure_osm_wt,
    nodes_gdf=nodes_gdf_osm_wt,
    distances=distances,
    jitter_scale=20,
)

# %%
# merge OSM
nearest_points_osm = nodes_gdf.geometry.apply(lambda geom: nodes_gdf_osm.geometry.distance(geom).idxmin())
nearest_geometries_osm = nodes_gdf_osm.loc[nearest_points_osm].geometry.reset_index(drop=True)
nodes_gdf["index_osm"] = nearest_points_osm.values
combined_gdf = nodes_gdf.merge(nodes_gdf_osm, left_on="index_osm", right_index=True, suffixes=("", "_osm"))

# %%
# merge OSM cleaned
nearest_points_osm_cleaned = combined_gdf.geometry.apply(
    lambda geom: nodes_gdf_osm_cleaned.geometry.distance(geom).idxmin()
)
nearest_geometries_osm_cleaned = nodes_gdf_osm_cleaned.loc[nearest_points_osm_cleaned].geometry.reset_index(drop=True)
combined_gdf["index_osm_cleaned"] = nearest_points_osm_cleaned.values
combined_gdf = combined_gdf.merge(
    nodes_gdf_osm_cleaned, left_on="index_osm_cleaned", right_index=True, suffixes=("", "_osm_cleaned")
)

# %%
# merge OSM wt
nearest_points_osm_wt = combined_gdf.geometry.apply(lambda geom: nodes_gdf_osm_wt.geometry.distance(geom).idxmin())
nearest_geometries_osm_wt = nodes_gdf_osm_wt.loc[nearest_points_osm_wt].geometry.reset_index(drop=True)
combined_gdf["index_osm_wt"] = nearest_points_osm_wt.values
combined_gdf = combined_gdf.merge(nodes_gdf_osm_wt, left_on="index_osm_wt", right_index=True, suffixes=("", "_osm_wt"))

# %%
# Create a 4x3 grid of subplots
fig, axs = plt.subplots(4, 3, figsize=(12, 20))
# Define combinations for scatter plots, adjust as per your actual data columns
combinations = [
    ("cc_metric_node_beta_500", "cc_metric_node_beta_500_osm"),
    ("cc_metric_node_beta_500", "cc_metric_node_beta_500_osm_cleaned"),
    ("cc_metric_node_beta_500", "cc_metric_node_beta_500_osm_wt"),
    ("cc_metric_node_beta_2000", "cc_metric_node_beta_2000_osm"),
    ("cc_metric_node_beta_2000", "cc_metric_node_beta_2000_osm_cleaned"),
    ("cc_metric_node_beta_2000", "cc_metric_node_beta_2000_osm_wt"),
    ("cc_metric_node_betweenness_500", "cc_metric_node_betweenness_500_osm"),
    ("cc_metric_node_betweenness_500", "cc_metric_node_betweenness_500_osm_cleaned"),
    ("cc_metric_node_betweenness_500", "cc_metric_node_betweenness_500_osm_wt"),
    ("cc_metric_node_betweenness_2000", "cc_metric_node_betweenness_2000_osm"),
    ("cc_metric_node_betweenness_2000", "cc_metric_node_betweenness_2000_osm_cleaned"),
    ("cc_metric_node_betweenness_2000", "cc_metric_node_betweenness_2000_osm_wt"),
]
for idx, (x, y) in enumerate(combinations):
    row = idx // 3
    col = idx % 3
    axs[row, col].scatter(combined_gdf[x], combined_gdf[y], alpha=0.5, s=0.4)
    axs[row, col].set_xlabel(x)
    axs[row, col].set_ylabel(y)
    axs[row, col].set_xticks([])
    axs[row, col].set_yticks([])
# Adjust the layout and show the plots
plt.tight_layout()
plt.show()

# %%
# Create a 4x3 grid of subplots
fig, axs = plt.subplots(4, 3, figsize=(12, 20))
# Define combinations for scatter plots, adjust as per your actual data columns
combinations = [
    ("cc_metric_node_harmonic_simplest_500", "cc_metric_node_harmonic_simplest_500_osm"),
    ("cc_metric_node_harmonic_simplest_500", "cc_metric_node_harmonic_simplest_500_osm_cleaned"),
    ("cc_metric_node_harmonic_simplest_500", "cc_metric_node_harmonic_simplest_500_osm_wt"),
    ("cc_metric_node_harmonic_simplest_2000", "cc_metric_node_harmonic_simplest_2000_osm"),
    ("cc_metric_node_harmonic_simplest_2000", "cc_metric_node_harmonic_simplest_2000_osm_cleaned"),
    ("cc_metric_node_harmonic_simplest_2000", "cc_metric_node_harmonic_simplest_2000_osm_wt"),
    ("cc_metric_node_betweenness_simplest_500", "cc_metric_node_betweenness_simplest_500_osm"),
    ("cc_metric_node_betweenness_simplest_500", "cc_metric_node_betweenness_simplest_500_osm_cleaned"),
    ("cc_metric_node_betweenness_simplest_500", "cc_metric_node_betweenness_simplest_500_osm_wt"),
    ("cc_metric_node_betweenness_simplest_2000", "cc_metric_node_betweenness_simplest_2000_osm"),
    ("cc_metric_node_betweenness_simplest_2000", "cc_metric_node_betweenness_simplest_2000_osm_cleaned"),
    ("cc_metric_node_betweenness_simplest_2000", "cc_metric_node_betweenness_simplest_2000_osm_wt"),
]
for idx, (x, y) in enumerate(combinations):
    row = idx // 3
    col = idx % 3
    axs[row, col].scatter(combined_gdf[x], combined_gdf[y], alpha=0.5, s=0.4)
    axs[row, col].set_xlabel(x)
    axs[row, col].set_ylabel(y)
    axs[row, col].set_xticks([])
    axs[row, col].set_yticks([])
# Adjust the layout and show the plots
plt.tight_layout()
plt.show()

# %%
plot_buffer = 2000
plot_bbox: tuple[float, float, float, float] = centroid.buffer(plot_buffer).bounds
betas = rustalgos.betas_from_distances(distances)
avg_dists = rustalgos.avg_distances_for_betas(betas)

for d, _b, avg_d in zip(distances, betas, avg_dists, strict=True):
    fig, axes = plt.subplots(2, 2, figsize=(7, 8), dpi=200, facecolor=bg_colour)
    fig.suptitle(f"Gravity index: {d}m ({avg_d:.2f}m avg. toler.)", color=font_colour)
    plot.plot_scatter(
        axes[0][0],
        network_structure.node_xs,
        network_structure.node_ys,
        nodes_gdf[f"cc_metric_node_beta_{d}"],
        bbox_extents=plot_bbox,
        cmap_key="coolwarm",
        face_colour=bg_colour,
    )
    axes[0][0].set_title("Official network graph", fontsize=font_size, color=font_colour)
    plot.plot_scatter(
        axes[0][1],
        network_structure_osm.node_xs,
        network_structure_osm.node_ys,
        nodes_gdf_osm[f"cc_metric_node_beta_{d}"],
        bbox_extents=plot_bbox,
        cmap_key="coolwarm",
        face_colour=bg_colour,
    )
    axes[0][1].set_title("OSM network", fontsize=font_size, color=font_colour)
    plot.plot_scatter(
        axes[1][0],
        network_structure_osm_cleaned.node_xs,
        network_structure_osm_cleaned.node_ys,
        nodes_gdf_osm_cleaned[f"cc_metric_node_beta_{d}"],
        bbox_extents=plot_bbox,
        cmap_key="coolwarm",
        face_colour=bg_colour,
    )
    axes[1][0].set_title("OSM algo cleaned", fontsize=font_size, color=font_colour)
    plot.plot_scatter(
        axes[1][1],
        network_structure_osm_wt.node_xs,
        network_structure_osm_wt.node_ys,
        nodes_gdf_osm_wt[f"cc_metric_node_beta_{d}"],
        bbox_extents=plot_bbox,
        cmap_key="coolwarm",
        face_colour=bg_colour,
    )
    axes[1][1].set_title("OSM weighted", fontsize=font_size, color=font_colour)
    plt.tight_layout()
    plt.show()

for d, _b, avg_d in zip(distances, betas, avg_dists, strict=True):
    fig, axes = plt.subplots(2, 2, figsize=(7, 8), dpi=200, facecolor=bg_colour)
    fig.suptitle(f"Dist. wtd. betw. ({avg_d:.2f}m avg. toler.)", color=font_colour)
    plot.plot_scatter(
        axes[0][0],
        network_structure.node_xs,
        network_structure.node_ys,
        nodes_gdf[f"cc_metric_node_betweenness_{d}"],
        bbox_extents=plot_bbox,
        cmap_key="coolwarm",
        s_max=2,
        face_colour=bg_colour,
    )
    axes[0][0].set_title("Official network graph", fontsize=font_size, color=font_colour)
    plot.plot_scatter(
        axes[0][1],
        network_structure_osm.node_xs,
        network_structure_osm.node_ys,
        nodes_gdf_osm[f"cc_metric_node_betweenness_{d}"],
        bbox_extents=plot_bbox,
        cmap_key="coolwarm",
        s_max=2,
        face_colour=bg_colour,
    )
    axes[0][1].set_title("OSM network", fontsize=font_size, color=font_colour)
    plot.plot_scatter(
        axes[1][0],
        network_structure_osm_cleaned.node_xs,
        network_structure_osm_cleaned.node_ys,
        nodes_gdf_osm_cleaned[f"cc_metric_node_betweenness_{d}"],
        bbox_extents=plot_bbox,
        cmap_key="coolwarm",
        s_max=2,
        face_colour=bg_colour,
    )
    axes[1][0].set_title("OSM algo cleaned", fontsize=font_size, color=font_colour)
    plot.plot_scatter(
        axes[1][1],
        network_structure_osm_wt.node_xs,
        network_structure_osm_wt.node_ys,
        nodes_gdf_osm_wt[f"cc_metric_node_betweenness_{d}"],
        bbox_extents=plot_bbox,
        cmap_key="coolwarm",
        s_max=2,
        face_colour=bg_colour,
    )
    axes[1][1].set_title("OSM weighted", fontsize=font_size, color=font_colour)

    plt.tight_layout()
    plt.show()

# %%
for d, _b, _avg_d in zip(distances, betas, avg_dists, strict=True):
    fig, axes = plt.subplots(2, 2, figsize=(7, 8), dpi=200, facecolor=bg_colour)
    fig.suptitle(f"Simplest path closeness centrality {d}m", color=font_colour)
    plot.plot_scatter(
        axes[0][0],
        network_structure.node_xs,
        network_structure.node_ys,
        nodes_gdf[f"cc_metric_node_harmonic_simplest_{d}"],
        bbox_extents=plot_bbox,
        cmap_key="coolwarm",
        face_colour=bg_colour,
    )
    axes[0][0].set_title("Official network graph", fontsize=font_size, color=font_colour)
    plot.plot_scatter(
        axes[0][1],
        network_structure_osm.node_xs,
        network_structure_osm.node_ys,
        nodes_gdf_osm[f"cc_metric_node_harmonic_simplest_{d}"],
        bbox_extents=plot_bbox,
        cmap_key="coolwarm",
        face_colour=bg_colour,
    )
    axes[0][1].set_title("OSM network", fontsize=font_size, color=font_colour)
    plot.plot_scatter(
        axes[1][0],
        network_structure_osm_cleaned.node_xs,
        network_structure_osm_cleaned.node_ys,
        nodes_gdf_osm_cleaned[f"cc_metric_node_harmonic_simplest_{d}"],
        bbox_extents=plot_bbox,
        cmap_key="coolwarm",
        face_colour=bg_colour,
    )
    axes[1][0].set_title("OSM algo cleaned", fontsize=font_size, color=font_colour)
    plot.plot_scatter(
        axes[1][1],
        network_structure_osm_wt.node_xs,
        network_structure_osm_wt.node_ys,
        nodes_gdf_osm_wt[f"cc_metric_node_harmonic_simplest_{d}"],
        bbox_extents=plot_bbox,
        cmap_key="coolwarm",
        face_colour=bg_colour,
    )
    axes[1][1].set_title("OSM weighted", fontsize=font_size, color=font_colour)
    plt.tight_layout()
    plt.show()

for d, _b, _avg_d in zip(distances, betas, avg_dists, strict=True):
    fig, axes = plt.subplots(2, 2, figsize=(7, 8), dpi=200, facecolor=bg_colour)
    fig.suptitle(f"Simplest path betweenness {d}m", color=font_colour)
    plot.plot_scatter(
        axes[0][0],
        network_structure.node_xs,
        network_structure.node_ys,
        nodes_gdf[f"cc_metric_node_betweenness_simplest_{d}"],
        bbox_extents=plot_bbox,
        cmap_key="coolwarm",
        s_max=2,
        face_colour=bg_colour,
    )
    axes[0][0].set_title("Official network graph", fontsize=font_size, color=font_colour)
    plot.plot_scatter(
        axes[0][1],
        network_structure_osm.node_xs,
        network_structure_osm.node_ys,
        nodes_gdf_osm[f"cc_metric_node_betweenness_simplest_{d}"],
        bbox_extents=plot_bbox,
        cmap_key="coolwarm",
        s_max=2,
        face_colour=bg_colour,
    )
    axes[0][1].set_title("OSM network", fontsize=font_size, color=font_colour)
    plot.plot_scatter(
        axes[1][0],
        network_structure_osm_cleaned.node_xs,
        network_structure_osm_cleaned.node_ys,
        nodes_gdf_osm_cleaned[f"cc_metric_node_betweenness_simplest_{d}"],
        bbox_extents=plot_bbox,
        cmap_key="coolwarm",
        s_max=2,
        face_colour=bg_colour,
    )
    axes[1][0].set_title("OSM algo cleaned", fontsize=font_size, color=font_colour)
    plot.plot_scatter(
        axes[1][1],
        network_structure_osm_wt.node_xs,
        network_structure_osm_wt.node_ys,
        nodes_gdf_osm_wt[f"cc_metric_node_betweenness_simplest_{d}"],
        bbox_extents=plot_bbox,
        cmap_key="coolwarm",
        s_max=2,
        face_colour=bg_colour,
    )
    axes[1][1].set_title("OSM weighted", fontsize=font_size, color=font_colour)
    plt.tight_layout()
    plt.show()
