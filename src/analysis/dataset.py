"""
Based on https://github.com/songololo/phd
"""

# %%
from __future__ import annotations

import os
from pathlib import Path

import geopandas as gpd

if str(Path.cwd()).endswith("analysis"):
    os.chdir("../..")
if not str(Path.cwd()).endswith("toolkit"):
    raise OSError(f"Check your working directory, currently: {print(Path.cwd())}")

from src import tools

engine = tools.get_sqlalchemy_engine()
logger = tools.get_logger(__name__)

# centrality columns
cent_cols_templ = [
    "cc_metric_node_beta_{d}",
    "cc_metric_node_cycles_{d}",
    "cc_metric_node_density_{d}",
    "cc_metric_node_farness_{d}",
    "cc_metric_node_harmonic_{d}",
    "cc_metric_node_hillier_{d}",
    "cc_metric_node_betweenness_{d}",
    "cc_metric_node_betweenness_beta_{d}",
    "cc_metric_node_density_simplest_{d}",
    "cc_metric_node_harmonic_simplest_{d}",
    "cc_metric_node_hillier_simplest_{d}",
    "cc_metric_node_farness_simplest_{d}",
    "cc_metric_node_betweenness_simplest_{d}",
]
cent_distances = [500, 1000, 2000, 5000, 10000]
cent_cols = []
for d in cent_distances:
    for col in cent_cols_templ:
        cent_cols.append(col.format(d=d))
# green columns
green_cols_templ = [
    "trees_{d}",
    "green_{d}",
]
green_distances = [100, 500]
green_cols = []
for d in green_distances:
    for col in green_cols_templ:
        green_cols.append(col.format(d=d))
# landuse columns
lu_cols_templ = [
    "cc_metric_restaurant_{d}_weighted",
    "cc_metric_restaurant_{d}_distance",
    "cc_metric_bar_{d}_weighted",
    "cc_metric_bar_{d}_distance",
    "cc_metric_cafe_{d}_weighted",
    "cc_metric_cafe_{d}_distance",
    "cc_metric_accommodation_{d}_weighted",
    "cc_metric_accommodation_{d}_distance",
    "cc_metric_automotive_{d}_weighted",
    "cc_metric_automotive_{d}_distance",
    "cc_metric_arts_and_entertainment_{d}_weighted",
    "cc_metric_arts_and_entertainment_{d}_distance",
    "cc_metric_attractions_and_activities_{d}_weighted",
    "cc_metric_attractions_and_activities_{d}_distance",
    "cc_metric_active_life_{d}_weighted",
    "cc_metric_active_life_{d}_distance",
    "cc_metric_beauty_and_spa_{d}_weighted",
    "cc_metric_beauty_and_spa_{d}_distance",
    "cc_metric_education_{d}_weighted",
    "cc_metric_education_{d}_distance",
    "cc_metric_financial_service_{d}_weighted",
    "cc_metric_financial_service_{d}_distance",
    "cc_metric_private_establishments_and_corporates_{d}_weighted",
    "cc_metric_private_establishments_and_corporates_{d}_distance",
    "cc_metric_retail_{d}_weighted",
    "cc_metric_retail_{d}_distance",
    "cc_metric_health_and_medical_{d}_weighted",
    "cc_metric_health_and_medical_{d}_distance",
    "cc_metric_pets_{d}_weighted",
    "cc_metric_pets_{d}_distance",
    "cc_metric_business_to_business_{d}_weighted",
    "cc_metric_business_to_business_{d}_distance",
    "cc_metric_public_service_and_government_{d}_weighted",
    "cc_metric_public_service_and_government_{d}_distance",
    "cc_metric_religious_organization_{d}_weighted",
    "cc_metric_religious_organization_{d}_distance",
    "cc_metric_real_estate_{d}_weighted",
    "cc_metric_real_estate_{d}_distance",
    "cc_metric_travel_{d}_weighted",
    "cc_metric_travel_{d}_distance",
    "cc_metric_professional_services_{d}_weighted",
    "cc_metric_professional_services_{d}_distance",
    "cc_metric_hill_wt_q0_{d}",
    "cc_metric_hill_wt_q1_{d}",
    "cc_metric_hill_wt_q2_{d}",
]
lu_distances = [100, 500, 1500]
lu_cols = []
for d in lu_distances:
    for col in lu_cols_templ:
        lu_cols.append(col.format(d=d))
# morphology columns
morph_cols_templ = [
    "cc_metric_area_mean_wt_{d}",
    "cc_metric_perimeter_mean_wt_{d}",
    "cc_metric_compactness_mean_wt_{d}",
    "cc_metric_orientation_mean_wt_{d}",
    "cc_metric_block_area_mean_wt_{d}",
    "cc_metric_block_perimeter_mean_wt_{d}",
    "cc_metric_block_compactness_mean_wt_{d}",
    "cc_metric_block_orientation_mean_wt_{d}",
    "cc_metric_block_covered_ratio_mean_wt_{d}",
]
morph_distances = [100, 500]
morph_cols = []
for d in morph_distances:
    for col in morph_cols_templ:
        morph_cols.append(col.format(d=d))


# %%
data_gdf = gpd.read_postgis(
    f"""
    SELECT nnc.fid,
            {', '.join(cent_cols)},
            {', '.join(green_cols)},
            {', '.join(lu_cols)},
            {', '.join(morph_cols)},
            pop.pop_dens,
            nnc.edge_geom as geom
        FROM overture.network_nodes_clean nnc TABLESAMPLE SYSTEM (2)
        JOIN metrics.centrality mc ON nnc.fid = mc.fid
        JOIN metrics.green gr ON nnc.fid = gr.fid
        JOIN metrics.landuses lu ON nnc.fid = lu.fid
        JOIN metrics.morphology mo ON nnc.fid = mo.fid
        JOIN metrics.population pop ON nnc.fid = pop.fid;
    """,
    engine,
    index_col="fid",
    geom_col="geom",
)
data_gdf
