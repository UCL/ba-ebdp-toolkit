"""
This method is replaced with the rasterised -> convolution method for performance.
"""

import argparse

import geopandas as gpd
from tqdm import tqdm

from src import tools

logger = tools.get_logger(__name__)

OVERTURE_SCHEMA = tools.generate_overture_schema()


def process_green(
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
            c.ns_node_idx,
            c.x,
            c.y,
            ST_Contains(b.geom, c.geom) as live,
            c.weight,
            c.geom
        FROM overture.network_nodes_clean c, eu.{bounds_table} b
        WHERE b.{bounds_fid_col} = {bounds_fid}
            AND ST_Contains(b.geom, c.geom) -- use bounds no need for buffer
        """,
        engine,
        index_col="fid",
        geom_col="geom",
    )
    # track bounds
    nodes_gdf.loc[:, "bounds_key"] = "bounds"
    nodes_gdf.loc[:, "bounds_fid"] = bounds_fid
    # green spaces
    trees_gdf = gpd.read_postgis(
        f"""
        SELECT t.fid, t.geom
        FROM eu.trees t, eu.{bounds_table} b
        WHERE b.{bounds_fid_col} = {bounds_fid}
            AND ST_Contains(b.{bounds_geom_col}, t.geom)
        """,
        engine,
        index_col="fid",
        geom_col="geom",
    )
    # trees
    green_gdf = gpd.read_postgis(
        f"""
        SELECT bl.fid, bl.geom
        FROM eu.blocks bl, eu.{bounds_table} b
        WHERE b.{bounds_fid_col} = {bounds_fid}
            AND ST_Contains(b.{bounds_geom_col}, bl.geom)
            AND class_2018 in (
                'Arable land (annual crops)',
                'Complex and mixed cultivation patterns',
                'Forests',
                'Green urban areas',
                'Herbaceous vegetation associations (natural grassland, moors...)',
                'Open spaces with little or no vegetation (beaches, dunes, bare rocks, glaciers)',
                'Orchards at the fringe of urban classes',
                'Pastures',
                'Permanent crops (vineyards, fruit trees, olive groves)',
                'Sports and leisure facilities',
                'Water',
                'Wetlands'
            )
        """,
        engine,
        index_col="fid",
        geom_col="geom",
    )
    logger.info("Creating unary trees geom")
    tree_cover_unary = trees_gdf.geometry.unary_union.simplify(5)
    logger.info("Creating unary green spaces geom")
    green_space_unary = green_gdf.geometry.unary_union.simplify(5)
    for dist in [100, 500]:
        logger.info(f"Processing distance: {dist}")
        # Buffer the nodes
        nodes_gdf[f"geom_{dist}"] = nodes_gdf["geom"].apply(lambda x: x.buffer(dist))
        for node_idx, node_row in tqdm(nodes_gdf.iterrows(), total=len(nodes_gdf)):
            # intersect with the unary geoms
            nodes_gdf.loc[node_idx, f"tree_cover_{dist}"] = node_row[f"geom_{dist}"].intersection(tree_cover_unary).area
            nodes_gdf.loc[node_idx, f"green_space_{dist}"] = (
                node_row[f"geom_{dist}"].intersection(green_space_unary).area
            )
        nodes_gdf.drop(columns=[f"geom_{dist}"], inplace=True)
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


def compute_green_metrics(
    target_bounds_fids: list[int] | str,
    drop: bool = False,
):
    if not (
        tools.check_table_exists("overture", "network_nodes_clean")
        and tools.check_table_exists("overture", "network_edges_clean")
    ):
        raise OSError("The cleaned network nodes and edges tables need to be created prior to proceeding.")
    logger.info("Computing green metrics")
    tools.prepare_schema("metrics")
    load_key = "metrics_green"
    bounds_schema = "eu"
    # use eu bounds not unioned_bounds - use geom_2000 for geom column
    bounds_table = "bounds"
    bounds_geom_col = "geom_2000"
    bounds_fid_col = "fid"
    target_schema = "metrics"
    target_table = "green"
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
            core_function=process_green,
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
    python -m src.processing.metrics_green all
    """

    if False:
        parser = argparse.ArgumentParser(description="Compute green space and tree metrics.")
        parser.add_argument(
            "bounds_fid",
            type=tools.bounds_fid_type,
            help=("A bounds fid as int to load a specific bounds. Use 'all' to load all bounds."),
        )
        parser.add_argument("--drop", action="store_true", help="Whether to drop existing tables.")
        args = parser.parse_args()
        compute_green_metrics(
            args.bounds_fid,
            drop=args.drop,
        )
    else:
        bounds_fids = [0]
        compute_green_metrics(
            bounds_fids,
            drop=True,
        )
