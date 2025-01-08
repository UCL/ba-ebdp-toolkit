""" """

import argparse

from cityseer.metrics import networks
from tqdm import tqdm

from src import tools

logger = tools.get_logger(__name__)


def process_centrality(
    bounds_fid: int,
    bounds_geom_col: str,
    target_schema: str,
    target_table: str,
):
    engine = tools.get_sqlalchemy_engine()
    nodes_gdf, edges_gdf, network_structure = tools.load_bounds_fid_network_from_db(
        engine, bounds_fid, buffer_col=bounds_geom_col
    )
    # track bounds
    nodes_gdf.loc[:, "bounds_key"] = "bounds"
    nodes_gdf.loc[:, "bounds_fid"] = bounds_fid
    # compute centrality
    nodes_gdf = networks.node_centrality_shortest(
        network_structure, nodes_gdf, distances=[500, 1000, 2000, 5000, 10000]
    )
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


def compute_centrality_metrics(
    target_bounds_fids: list[int] | str,
    drop: bool = False,
):
    if not (tools.check_table_exists("overture", "dual_nodes") and tools.check_table_exists("overture", "dual_edges")):
        raise OSError("The cleaned network nodes and edges tables need to be created prior to proceeding.")
    logger.info("Computing network centrality")
    tools.prepare_schema("metrics")
    load_key = "metrics_centrality"
    bounds_schema = "eu"
    # use eu bounds not unioned_bounds - use geom_10000 for geom column
    bounds_table = "bounds"
    bounds_geom_col = "geom_10000"
    bounds_fid_col = "fid"
    target_schema = "metrics"
    target_table = "centrality"
    bounds_fids_geoms = tools.iter_boundaries(bounds_schema, bounds_table, bounds_fid_col, bounds_geom_col, wgs84=False)
    # target fids
    if target_bounds_fids == "all":
        target_fids = [int(big[0]) for big in bounds_fids_geoms]
    else:
        target_fids = [int(fid) for fid in target_bounds_fids]
    # iter
    for bound_fid, _ in tqdm(bounds_fids_geoms):
        if bound_fid not in target_fids:
            continue
        tools.process_func_with_bound_tracking(
            bound_fid=bound_fid,
            load_key=load_key,
            core_function=process_centrality,
            func_args=[
                bound_fid,
                bounds_geom_col,
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
    python -m src.processing.metrics_centrality all
    """

    if True:
        parser = argparse.ArgumentParser(description="Compute centrality metrics.")
        parser.add_argument(
            "bounds_fid",
            type=tools.bounds_fid_type,
            help=("A bounds fid as int to load a specific bounds. Use 'all' to load all bounds."),
        )
        parser.add_argument("--drop", action="store_true", help="Whether to drop existing tables.")
        args = parser.parse_args()
        compute_centrality_metrics(
            args.bounds_fid,
            drop=args.drop,
        )
    else:
        bounds_fids = [636]
        compute_centrality_metrics(
            bounds_fids,
            drop=True,
        )
