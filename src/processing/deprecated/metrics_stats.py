""" """

import argparse

import geopandas as gpd
import numpy as np
from scipy.interpolate import griddata
from tqdm import tqdm

from src import tools

logger = tools.get_logger(__name__)


def process_stats(
    bounds_fid: int,
    bounds_fid_col: str,
    bounds_geom_col: str,
    bounds_table: str,
    target_schema: str,
    target_table: str,
):
    engine = tools.get_sqlalchemy_engine()
    nodes_gdf = gpd.read_postgis(
        f"""
        SELECT
            c.fid,
            c.x,
            c.y,
            ST_Contains(b.geom, ST_Centroid(c.primal_edge)) as live,
            c.primal_edge as geom
        FROM overture.dual_nodes c, eu.{bounds_table} b
        WHERE b.{bounds_fid_col} = {bounds_fid}
                AND ST_Intersects(b.geom, c.primal_edge)
                AND ST_Contains(b.geom, ST_Centroid(c.primal_edge));
        """,
        engine,
        index_col="fid",
        geom_col="geom",
    )
    if len(nodes_gdf) == 0:  # type: ignore
        raise OSError(f"No network data for bounds FID: {bounds_fid}")
    # track bounds
    nodes_gdf.loc[:, "bounds_key"] = "bounds"  # type: ignore
    nodes_gdf.loc[:, "bounds_fid"] = bounds_fid  # type: ignore
    # fetch stats
    stats_gdf = gpd.read_postgis(
        f"""
        SELECT
            s.fid,
            s.t,
            s.m,
            s.f,
            s.y_lt15,
            s.y_1564,
            s.y_ge65,
            s.emp,
            s.nat,
            s.eu_oth,
            s.oth,
            s.same,
            s.chg_in,
            s.chg_out,
            ST_Centroid(s.geom) as cent
        FROM eu.stats s, eu.{bounds_table} b
        WHERE b.{bounds_fid_col} = {bounds_fid}
                AND ST_Intersects(b.{bounds_geom_col}, s.geom);
        """,
        engine,
        index_col="fid",
        geom_col="cent",
    )
    grid_coords = np.array([(point.x, point.y) for point in stats_gdf.cent])  # type: ignore
    target_coords = np.column_stack((nodes_gdf.x, nodes_gdf.y))  # type: ignore
    cols = [
        "t",
        "m",
        "f",
        "y_lt15",
        "y_1564",
        "y_ge65",
        "emp",
        "nat",
        "eu_oth",
        "oth",
        "same",
        "chg_in",
        "chg_out",
    ]
    for col in cols:
        grid_values = stats_gdf[col].values  # type: ignore
        nodes_gdf[col] = griddata(grid_coords, grid_values, target_coords, method="cubic")  # type: ignore
    # keep only live
    nodes_gdf = nodes_gdf.loc[nodes_gdf.live]  # type: ignore
    nodes_gdf.to_postgis(  # type: ignore
        target_table,
        engine,
        if_exists="append",
        schema=target_schema,
        index=True,
        index_label="fid",
    )


def compute_stats_metrics(
    target_bounds_fids: list[int] | str,
    drop: bool = False,
):
    if not (tools.check_table_exists("overture", "dual_nodes") and tools.check_table_exists("overture", "dual_edges")):
        raise OSError("The cleaned network nodes and edges tables need to be created prior to proceeding.")
    logger.info("Computing stats metrics")
    tools.prepare_schema("metrics")
    load_key = "metrics_stats"
    bounds_schema = "eu"
    # use eu bounds not unioned_bounds - use geom_2000 for geom column
    bounds_table = "bounds"
    bounds_geom_col = "geom_2000"
    bounds_fid_col = "fid"
    target_schema = "metrics"
    target_table = "stats"
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
            core_function=process_stats,
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
    python -m src.processing.metrics_stats all
    """

    if True:
        parser = argparse.ArgumentParser(description="Compute stats metrics.")
        parser.add_argument(
            "bounds_fid",
            type=tools.bounds_fid_type,
            help=("A bounds fid as int to load a specific bounds. Use 'all' to load all bounds."),
        )
        parser.add_argument("--drop", action="store_true", help="Whether to drop existing tables.")
        args = parser.parse_args()
        compute_stats_metrics(
            args.bounds_fid,
            drop=args.drop,
        )
    else:
        bounds_fids = [636]
        compute_stats_metrics(
            bounds_fids,
            drop=True,
        )
