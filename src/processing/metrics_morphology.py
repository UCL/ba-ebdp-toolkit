""" """

from __future__ import annotations

import argparse

import geopandas as gpd
import momepy
import numpy as np
from cityseer.metrics import layers
from cityseer.tools import io
from tqdm import tqdm

from src import tools

logger = tools.get_logger(__name__)


def process_morphology(
    bounds_fid: int,
    bounds_fid_col: str,
    bounds_geom_col: str,
    bounds_table: str,
    target_schema: str,
    target_table: str,
):
    engine = tools.get_sqlalchemy_engine()
    multigraph = tools.load_bounds_fid_network_from_db(engine, bounds_fid, buffer_col=bounds_geom_col)
    if len(multigraph) == 0:
        raise OSError(f"No network data for bounds FID: {bounds_fid}")
    nodes_gdf, _edges_gdf, network_structure = io.network_structure_from_nx(multigraph, crs=3035)
    # track bounds
    nodes_gdf.loc[:, "bounds_key"] = "bounds"
    nodes_gdf.loc[:, "bounds_fid"] = bounds_fid
    # buildings
    bldgs_gdf = gpd.read_postgis(
        f"""
        SELECT bldgs.fid, bldgs.geom
        FROM overture.overture_buildings bldgs, eu.{bounds_table} b
        WHERE b.{bounds_fid_col} = {bounds_fid}
            AND ST_Contains(b.{bounds_geom_col}, bldgs.geom);
        """,
        engine,
        index_col="fid",
        geom_col="geom",
    )
    if bldgs_gdf.empty is True:
        logger.warning(f"No buildings data found for city {bounds_fid}")
        # placeholders
        for col_key in ["area", "perimeter", "compactness", "orientation"]:
            bldgs_gdf[col_key] = np.nan
    else:
        # explode
        bldgs_gdf = bldgs_gdf.explode(index_parts=False)
        bldgs_gdf.reset_index(drop=True, inplace=True)
        bldgs_gdf.index = bldgs_gdf.index.astype(str)
        # bldg metrics
        bldgs_gdf["area"] = momepy.Area(bldgs_gdf).series
        bldgs_gdf["perimeter"] = momepy.Perimeter(bldgs_gdf).series
        bldgs_gdf["compactness"] = momepy.CircularCompactness(bldgs_gdf, "area").series
        bldgs_gdf["orientation"] = momepy.Orientation(bldgs_gdf).series
    # calculate
    bldgs_gdf["centroid"] = bldgs_gdf.geometry.centroid
    bldgs_gdf.set_geometry("centroid", inplace=True)
    for col_key in ["area", "perimeter", "compactness", "orientation"]:
        nodes_gdf, bldgs_gdf = layers.compute_stats(
            data_gdf=bldgs_gdf,
            stats_column_label=col_key,
            nodes_gdf=nodes_gdf,
            network_structure=network_structure,
            distances=[100, 500, 1500],
        )
        trim_columns = []
        for column_name in nodes_gdf.columns:
            if col_key in column_name and not column_name.startswith(f"cc_metric_{col_key}_mean_wt"):
                trim_columns.append(column_name)
        nodes_gdf.drop(columns=trim_columns, inplace=True)
    # blocks
    blocks_gdf = gpd.read_postgis(
        f"""
        SELECT bl.fid, bl.geom
        FROM eu.blocks bl, eu.{bounds_table} b
        WHERE b.{bounds_fid_col} = {bounds_fid}
            AND ST_Contains(b.{bounds_geom_col}, bl.geom);
        """,
        engine,
        index_col="fid",
        geom_col="geom",
    )
    if blocks_gdf.empty is True or bldgs_gdf.empty is True:
        logger.warning(f"No blocks or buildings data found for city {bounds_fid}")
        # placeholders
        for col_key in [
            "block_area",
            "block_perimeter",
            "block_compactness",
            "block_orientation",
            "block_covered_ratio",
        ]:
            blocks_gdf[col_key] = np.nan
    else:
        # block metrics
        blocks_gdf.index = blocks_gdf.index.astype(str)
        blocks_gdf["block_area"] = momepy.Area(blocks_gdf).series
        blocks_gdf["block_perimeter"] = momepy.Perimeter(blocks_gdf).series
        blocks_gdf["block_compactness"] = momepy.CircularCompactness(blocks_gdf, "block_area").series
        blocks_gdf["block_orientation"] = momepy.Orientation(blocks_gdf).series
        # spatial join
        merged_gdf = gpd.sjoin(bldgs_gdf, blocks_gdf, how="left", predicate="within", lsuffix="bldg", rsuffix="bl")
        blocks_gdf["index_bl"] = blocks_gdf.index.values
        blocks_gdf["block_covered_ratio"] = momepy.AreaRatio(
            blocks_gdf, merged_gdf, "block_area", "area", left_unique_id="index_bl", right_unique_id="index_bl"
        ).series
    # calculate
    blocks_gdf["centroid"] = blocks_gdf.geometry.centroid
    blocks_gdf.set_geometry("centroid", inplace=True)
    for col_key in ["block_area", "block_perimeter", "block_compactness", "block_orientation", "block_covered_ratio"]:
        nodes_gdf, blocks_gdf = layers.compute_stats(
            data_gdf=blocks_gdf,
            stats_column_label=col_key,
            nodes_gdf=nodes_gdf,
            network_structure=network_structure,
            distances=[100, 500, 1500],
        )
        trim_columns = []
        for column_name in nodes_gdf.columns:
            if col_key in column_name and not column_name.startswith(f"cc_metric_{col_key}_mean_wt"):
                trim_columns.append(column_name)
        nodes_gdf.drop(columns=trim_columns, inplace=True)
    # keep only live
    nodes_gdf = nodes_gdf.loc[nodes_gdf.live]
    nodes_gdf.to_postgis(  # type: ignore
        target_table,
        engine,
        if_exists="append",
        schema=target_schema,
        index=True,
        index_label="fid",
    )


def compute_morphology_metrics(
    target_bounds_fids: list[int] | str,
    drop: bool = False,
):
    if not (
        tools.check_table_exists("overture", "network_nodes_clean")
        and tools.check_table_exists("overture", "network_edges_clean")
    ):
        raise OSError("The cleaned network nodes and edges tables need to be created prior to proceeding.")
    logger.info("Computing building and block morphology metrics")
    tools.prepare_schema("metrics")
    load_key = "metrics_morphology"
    bounds_schema = "eu"
    # use eu bounds not unioned_bounds - use geom_10000 for geom column
    bounds_table = "bounds"
    bounds_geom_col = "geom_10000"
    bounds_fid_col = "fid"
    target_schema = "metrics"
    target_table = "morphology"
    bounds_fids_geoms = tools.iter_boundaries(bounds_schema, bounds_table, bounds_fid_col, bounds_geom_col, wgs84=False)
    # check fids
    bounds_fids = [big[0] for big in bounds_fids_geoms]
    if isinstance(target_bounds_fids, str) and target_bounds_fids == "all":
        process_fids = bounds_fids
    elif set(target_bounds_fids).issubset(set(bounds_fids)):
        process_fids = target_bounds_fids
    else:
        raise ValueError(
            'target_bounds_fids must either be "all" to load all boundaries '
            f"or should correspond to an fid found in {bounds_schema}.{bounds_table} table."
        )
    # iter
    for bound_fid in tqdm(process_fids):
        tools.process_func_with_bound_tracking(
            bound_fid=bound_fid,
            load_key=load_key,
            core_function=process_morphology,
            func_args=[
                bound_fid,
                bounds_fid_col,
                bounds_geom_col,
                bounds_table,
                target_schema,
                target_table,
            ],
            content_schema=target_schema,
            content_tables=[target_table],
            bounds_schema=bounds_schema,
            bounds_table=bounds_table,
            bounds_geom_col=bounds_geom_col,
            bounds_fid_col=bounds_fid_col,
            drop=drop,
        )


if __name__ == "__main__":
    """
    Examples are run from the project folder (the folder containing src)
    python -m src.processing.metrics_morphology all
    """
    if True:
        parser = argparse.ArgumentParser(description="Compute building and block morphology metrics.")
        parser.add_argument(
            "bounds_fid",
            type=tools.bounds_fid_type,
            help=("A bounds fid as int to load a specific bounds. Use 'all' to load all bounds."),
        )
        parser.add_argument("--drop", action="store_true", help="Whether to drop existing tables.")
        args = parser.parse_args()
        compute_morphology_metrics(
            args.bounds_fid,
            drop=args.drop,
        )
    else:
        bounds_fids = [15]
        compute_morphology_metrics(
            bounds_fids,
            drop=True,
        )
