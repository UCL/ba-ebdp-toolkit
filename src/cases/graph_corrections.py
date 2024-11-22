# %%
# !pip install --upgrade cityseer

# %%
import matplotlib.pyplot as plt
from cityseer import rustalgos
from cityseer.metrics import networks
from cityseer.tools import graphs, io, plot

# download from OSM
lng, lat = -0.13396079424572427, 51.51371088849723
buffer = 5000
distances = [250, 500, 1000, 2000]
# creates a WGS shapely polygon
poly_wgs, poly_utm, _utm_zone_number, _utm_zone_letter = io.buffered_point_poly(lng, lat, buffer)

# %% [markdown]
# ### Automatic cleaning
#
# This approach prepares a network using automated algorithmic cleaning methods to consolidate complex intersections
# and parallel roads.
#

# %%
G_utm = io.osm_graph_from_poly(
    poly_wgs,
    simplify=True,
)
# decompose for higher resolution analysis
G_decomp = graphs.nx_decompose(G_utm, 25)
# prepare data structures
nodes_gdf, _edges_gdf, network_structure = io.network_structure_from_nx(G_decomp, crs=32629)
# compute centralities
# if computing wider area centralities, e.g. 20km, then use less decomposition to speed up the computation
nodes_gdf = networks.node_centrality_shortest(
    network_structure=network_structure,
    nodes_gdf=nodes_gdf,
    distances=distances,
)
# compute simplest path centrality
nodes_gdf_simpl = networks.node_centrality_simplest(
    network_structure=network_structure,
    nodes_gdf=nodes_gdf,
    distances=distances,
)

# %% [markdown]
# ### Minimal cleaning
#
# This method performs minimal cleaning and is used for reference point for the other two methods.
#

# %%
# generate OSM graph from polygon - note no automatic simplification applied
G_utm_minimal = io.osm_graph_from_poly(poly_wgs, simplify=False)
# do minimal graph cleaning
G_utm_minimal = graphs.nx_remove_dangling_nodes(G_utm_minimal, despine=15)
# decompose for higher resolution analysis
G_decomp_minimal = graphs.nx_decompose(G_utm_minimal, 25)
# prepare data structures
nodes_gdf_minimal, _edges_gdf_minimal, network_structure_minimal = io.network_structure_from_nx(
    G_decomp_minimal, crs=32629
)
# compute centrality
nodes_gdf_minimal = networks.node_centrality_shortest(
    network_structure=network_structure_minimal,
    nodes_gdf=nodes_gdf_minimal,
    distances=distances,
)
# compute simplest path centrality
nodes_gdf_minimal_simpl = networks.node_centrality_simplest(
    network_structure=network_structure_minimal,
    nodes_gdf=nodes_gdf_minimal,
    distances=distances,
)

# %% [markdown]
# ### Dissolving network weights
#
# This approach doesn't attempt to consolidate the network. Instead, it uses techniques to control for messy network
# representations:
#
# - It "dissolves" network weights - meaning that nodes representing street segments which are likely duplicitous are
# weighted less heavily.
# - It injects "jitter" to derive more intuitively consistent network routes.
#

# %%
# generate dissolved weights
G_dissolved_wts = graphs.nx_weight_by_dissolved_edges(G_decomp_minimal)
# prepare data structures
nodes_gdf_dissolved, _edges_gdf_dissolved, network_structure_dissolved = io.network_structure_from_nx(
    G_dissolved_wts, crs=32629
)
# compute centralities
nodes_gdf_dissolved = networks.node_centrality_shortest(
    network_structure=network_structure_dissolved,
    nodes_gdf=nodes_gdf_dissolved,
    distances=distances,
    jitter_scale=20,
)
# compute simplest path centrality
# in this case jitter is angular, so 20 here refers to degrees
nodes_gdf_dissolved_simpl = networks.node_centrality_simplest(
    network_structure=network_structure_dissolved,
    nodes_gdf=nodes_gdf_dissolved,
    distances=distances,
    jitter_scale=20,
)

# %% [markdown]
# ### Plots
#
# Compares a selection of distance thresholds for each approach.
#

# %%
bg_colour = "#111"
betas = rustalgos.betas_from_distances(distances)
avg_dists = rustalgos.avg_distances_for_betas(betas)
plot_bbox = poly_utm.centroid.buffer(1500).bounds
font_size = 7
font_color = "lightgrey"
for d, b, avg_d in zip(distances, betas, avg_dists, strict=True):
    print(
        f"""
    "Gravity" index (spatial impedance weighted closeness-like centrality):
    Avg walking tolerance: {avg_d:.2f}m
    Beta: {b:.3f} (spatial impedance factor)
    Max walking tolerance: {d:.1f}m
    """
    )
    fig, axes = plt.subplots(1, 3, figsize=(8, 4), dpi=200, facecolor=bg_colour)
    fig.suptitle(f"Dist. wtd. shortest path gravity index: {d}m ({avg_d:.2f}m avg. tolerance)", color=font_color)
    plot.plot_scatter(
        axes[0],
        network_structure.node_xs,
        network_structure.node_ys,
        nodes_gdf[f"cc_metric_node_beta_{d}"],
        bbox_extents=plot_bbox,
        cmap_key="magma",
        face_colour=bg_colour,
    )
    axes[0].set_title("Algorithmically cleaned network", fontsize=font_size, color=font_color)
    plot.plot_scatter(
        axes[1],
        network_structure_minimal.node_xs,
        network_structure_minimal.node_ys,
        nodes_gdf_minimal[f"cc_metric_node_beta_{d}"],
        bbox_extents=plot_bbox,
        cmap_key="magma",
        face_colour=bg_colour,
    )
    axes[1].set_title("Minimally cleaned network", fontsize=font_size, color=font_color)
    plot.plot_scatter(
        axes[2],
        network_structure_dissolved.node_xs,
        network_structure_dissolved.node_ys,
        nodes_gdf_dissolved[f"cc_metric_node_beta_{d}"],
        bbox_extents=plot_bbox,
        cmap_key="magma",
        face_colour=bg_colour,
    )
    axes[2].set_title("Minimal w. dissolved edge weightings and jitter", fontsize=font_size, color=font_color)
    plt.tight_layout()
    plt.show()

for d, b, avg_d in zip(distances, betas, avg_dists, strict=True):
    print(
        f"""
    Spatial impedance weighted betweenness centrality:
    Avg walking tolerance: {avg_d:.2f}m
    Beta: {b:.3f} (spatial impedance factor)
    Max walking tolerance: {d:.1f}m
    """
    )
    fig, axes = plt.subplots(1, 3, figsize=(8, 4), dpi=200, facecolor=bg_colour)
    fig.suptitle(
        f"Dist. wtd. shortest path betweenness centrality: {d}m ({avg_d:.2f}m avg. tolerance)", color=font_color
    )
    plot.plot_scatter(
        axes[0],
        network_structure.node_xs,
        network_structure.node_ys,
        nodes_gdf[f"cc_metric_node_betweenness_{d}"],
        bbox_extents=plot_bbox,
        cmap_key="magma",
        s_max=2,
        face_colour=bg_colour,
    )
    axes[0].set_title("Algorithmically cleaned network", fontsize=font_size, color=font_color)
    plot.plot_scatter(
        axes[1],
        network_structure_minimal.node_xs,
        network_structure_minimal.node_ys,
        nodes_gdf_minimal[f"cc_metric_node_betweenness_{d}"],
        bbox_extents=plot_bbox,
        cmap_key="magma",
        s_max=2,
        face_colour=bg_colour,
    )
    axes[1].set_title("Minimally cleaned network", fontsize=font_size, color=font_color)
    plot.plot_scatter(
        axes[2],
        network_structure_dissolved.node_xs,
        network_structure_dissolved.node_ys,
        nodes_gdf_dissolved[f"cc_metric_node_betweenness_{d}"],
        bbox_extents=plot_bbox,
        cmap_key="magma",
        s_max=2,
        face_colour=bg_colour,
    )
    axes[2].set_title("Minimal w. dissolved edge weightings and jitter", fontsize=font_size, color=font_color)
    plt.tight_layout()
    plt.show()

# %%
for d, _b, _avg_d in zip(distances, betas, avg_dists, strict=True):
    print(
        f"""
    "Simplest path harmonic closeness centrality
    (Locally corrected form of closeness centrality)
    Distance: {d:.1f}m
    """
    )
    fig, axes = plt.subplots(1, 3, figsize=(8, 4), dpi=200, facecolor=bg_colour)
    fig.suptitle(f"Simplest path closeness centrality: {d}m", color=font_color)
    plot.plot_scatter(
        axes[0],
        network_structure.node_xs,
        network_structure.node_ys,
        nodes_gdf_simpl[f"cc_metric_node_harmonic_simplest_{d}"],
        bbox_extents=plot_bbox,
        cmap_key="magma",
        face_colour=bg_colour,
    )
    axes[0].set_title("Algorithmically cleaned network", fontsize=font_size, color=font_color)
    plot.plot_scatter(
        axes[1],
        network_structure_minimal.node_xs,
        network_structure_minimal.node_ys,
        nodes_gdf_minimal_simpl[f"cc_metric_node_harmonic_simplest_{d}"],
        bbox_extents=plot_bbox,
        cmap_key="magma",
        face_colour=bg_colour,
    )
    axes[1].set_title("Minimally cleaned network", fontsize=font_size, color=font_color)
    plot.plot_scatter(
        axes[2],
        network_structure_dissolved.node_xs,
        network_structure_dissolved.node_ys,
        nodes_gdf_dissolved_simpl[f"cc_metric_node_harmonic_simplest_{d}"],
        bbox_extents=plot_bbox,
        cmap_key="magma",
        face_colour=bg_colour,
    )
    axes[2].set_title("Minimal w. dissolved edge weightings and jitter", fontsize=font_size, color=font_color)
    plt.tight_layout()
    plt.show()

for d, _b, _avg_d in zip(distances, betas, avg_dists, strict=True):
    print(
        f"""
    Simplest path betweenness centrality:
    Distance: {d:.1f}m
    """
    )
    fig, axes = plt.subplots(1, 3, figsize=(8, 4), dpi=200, facecolor=bg_colour)
    fig.suptitle(f"Simplest path betweenness centrality: {d}m", color=font_color)
    plot.plot_scatter(
        axes[0],
        network_structure.node_xs,
        network_structure.node_ys,
        nodes_gdf_simpl[f"cc_metric_node_betweenness_simplest_{d}"],
        bbox_extents=plot_bbox,
        cmap_key="magma",
        s_max=2,
        face_colour=bg_colour,
    )
    axes[0].set_title("Algorithmically cleaned network", fontsize=font_size, color=font_color)
    plot.plot_scatter(
        axes[1],
        network_structure_minimal.node_xs,
        network_structure_minimal.node_ys,
        nodes_gdf_minimal_simpl[f"cc_metric_node_betweenness_simplest_{d}"],
        bbox_extents=plot_bbox,
        cmap_key="magma",
        s_max=2,
        face_colour=bg_colour,
    )
    axes[1].set_title("Minimally cleaned network", fontsize=font_size, color=font_color)
    plot.plot_scatter(
        axes[2],
        network_structure_dissolved.node_xs,
        network_structure_dissolved.node_ys,
        nodes_gdf_dissolved_simpl[f"cc_metric_node_betweenness_simplest_{d}"],
        bbox_extents=plot_bbox,
        cmap_key="magma",
        s_max=2,
        face_colour=bg_colour,
    )
    axes[2].set_title("Minimal w. dissolved edge weightings and jitter", fontsize=font_size, color=font_color)
    plt.tight_layout()
    plt.show()
