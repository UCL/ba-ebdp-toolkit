""" """

# %%
from __future__ import annotations

import os
from pathlib import Path

import geopandas as gpd

if str(Path.cwd()).endswith("analysis"):
    os.chdir("../..")
if not str(Path.cwd()).endswith("toolkit"):
    raise IOError(f"Check your working directory, currently: {print(Path.cwd())}")

from src import tools

engine = tools.get_sqlalchemy_engine()
logger = tools.get_logger(__name__)

# %%
data_gdf = gpd.read_postgis(
    f"""
    SELECT mc.*, gr.*, lu.*, mo.*, pop.*, nnc.edge_geom as geom
        FROM overture.network_nodes_clean nnc TABLESAMPLE BERNOULLI (5) REPEATABLE (0)
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
