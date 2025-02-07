""" """

import argparse

import geopandas as gpd
import numpy as np
from scipy.interpolate import griddata
from tqdm import tqdm

from src import tools
from src.processing import processors

logger = tools.get_logger(__name__)


def generate_metrics(
    bounds_fid: int,
    bounds_fid_col: str,
    bounds_table: str,
    target_schema: str,
    target_nodes_table: str,
    target_blocks_table: str,
    target_bldgs_table: str,
):
    engine = tools.get_sqlalchemy_engine()
    nodes_gdf, edges_gdf, network_structure = tools.load_bounds_fid_network_from_db(
        engine, bounds_fid, buffer_col="geom_10000"
    )
    # CENTRALITY
    nodes_gdf = processors.process_centrality(nodes_gdf, network_structure)
    # POI
    places_gdf: gpd.GeoDataFrame = gpd.read_postgis(  # type: ignore
        f"""
        SELECT p.fid, p.main_cat, p.geom
        FROM overture.overture_place p, eu.{bounds_table} b
        WHERE b.{bounds_fid_col} = {bounds_fid}
            AND ST_Intersects(b.geom_2000, p.geom)
        """,
        engine,
        index_col="fid",
        geom_col="geom",
    )
    infrast_gdf: gpd.GeoDataFrame = gpd.read_postgis(  # type: ignore
        f"""
        SELECT p.fid, p.class, p.geom
        FROM overture.overture_infrast p, eu.{bounds_table} b
        WHERE b.{bounds_fid_col} = {bounds_fid}
            AND ST_Contains(b.geom_2000, p.geom)
        """,
        engine,
        index_col="fid",
        geom_col="geom",
    )
    nodes_gdf = processors.process_places(nodes_gdf, places_gdf, infrast_gdf, network_structure)
    # BUILDINGS
    # load raster
    raster_bytes = tools.db_fetch(f"""
        SELECT ST_AsGDALRaster(ST_Union(ST_Clip(rast, b.geom_2000)), 'GTiff') AS rast
        FROM eu.bldg_hts, eu.{bounds_table} b
        WHERE b.{bounds_fid_col} = {bounds_fid}
            AND ST_Intersects(b.geom_2000, rast);
        """)[0][0]
    # buildings
    bldgs_gdf: gpd.GeoDataFrame = gpd.read_postgis(  # type: ignore
        f"""
        SELECT bldgs.fid, bldgs.geom
        FROM overture.overture_buildings bldgs, eu.{bounds_table} b
        WHERE b.{bounds_fid_col} = {bounds_fid}
            AND ST_Intersects(b.geom_2000, bldgs.geom);
        """,
        engine,
        index_col="fid",
        geom_col="geom",
    )
    # blocks
    blocks_gdf: gpd.GeoDataFrame = gpd.read_postgis(  # type: ignore
        f"""
        SELECT bl.fid, bl.geom
        FROM eu.blocks bl, eu.{bounds_table} b
        WHERE b.{bounds_fid_col} = {bounds_fid}
            AND ST_Contains(b.geom_2000, bl.geom);
        """,
        engine,
        index_col="fid",
        geom_col="geom",
    )
    nodes_gdf, bldgs_gdf, blocks_gdf = processors.process_blocks_buildings(
        nodes_gdf, bldgs_gdf, blocks_gdf, raster_bytes, network_structure
    )
    if not bldgs_gdf.empty:
        bldgs_gdf["bounds_key"] = "bounds"
        bldgs_gdf["bounds_fid"] = bounds_fid
        bldgs_gdf.to_postgis(
            target_bldgs_table,
            engine,
            if_exists="append",
            schema="metrics",
            index=True,
            index_label="fid",
        )
    if not blocks_gdf.empty:
        blocks_gdf["bounds_key"] = "bounds"
        blocks_gdf["bounds_fid"] = bounds_fid
        blocks_gdf.to_postgis(
            target_blocks_table,
            engine,
            if_exists="append",
            schema="metrics",
            index=True,
            index_label="fid",
        )
    # GREEN
    # green spaces
    green_gdf: gpd.GeoDataFrame = gpd.read_postgis(  # type: ignore
        f"""
        SELECT bl.fid, bl.geom
        FROM eu.blocks bl, eu.{bounds_table} b
        WHERE b.{bounds_fid_col} = {bounds_fid}
            -- use intersects to catch overlapping geoms
            AND ST_Intersects(b.geom_2000, bl.geom)
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
            AND ST_IsValid(bl.geom)
        """,
        engine,
        index_col="fid",
        geom_col="geom",
    )
    # trees - simplify
    trees_gdf: gpd.GeoDataFrame = gpd.read_postgis(  # type: ignore
        f"""
        SELECT t.fid, t.geom
        FROM eu.trees t, eu.{bounds_table} b
        WHERE b.{bounds_fid_col} = {bounds_fid}
            -- use intersects to catch overlapping geoms
            AND ST_Intersects(b.geom_2000, t.geom)
            AND ST_IsValid(t.geom)
        """,
        engine,
        index_col="fid",
        geom_col="geom",
    )
    nodes_gdf = processors.process_green(nodes_gdf, green_gdf, trees_gdf, network_structure)
    # STATS
    logger.info("Computing stats")
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
                AND ST_Intersects(b.geom_2000, s.geom);
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
    for col in tqdm(cols):
        grid_values = stats_gdf[col].values  # type: ignore
        # use linear because cubic goes negative
        nodes_gdf[col] = griddata(grid_coords, grid_values, target_coords, method="linear")  # type: ignore
    # keep only live
    if not nodes_gdf.empty:
        nodes_gdf["bounds_key"] = "bounds"
        nodes_gdf["bounds_fid"] = bounds_fid
        nodes_gdf = nodes_gdf.loc[nodes_gdf.live]
        nodes_gdf.to_postgis(  # type: ignore
            target_nodes_table,
            engine,
            if_exists="append",
            schema=target_schema,
            index=True,
            index_label="fid",
        )


def compute_metrics(
    target_bounds_fids: list[int] | str,
    drop: bool = False,
):
    for schema, table in [
        ("overture", "dual_nodes"),
        ("overture", "dual_edges"),
        ("overture", "overture_place"),
        ("overture", "overture_infrast"),
        ("overture", "overture_buildings"),
        ("eu", "bounds"),
        ("eu", "bldg_hts"),
        ("eu", "blocks"),
        ("eu", "trees"),
        ("eu", "stats"),
    ]:
        if not (tools.check_table_exists(schema, table)):
            raise OSError(f"The {schema}.{table} table needs to be created prior to proceeding.")
    logger.info("Computing metrics")
    tools.prepare_schema("metrics")
    load_key = "metrics"
    bounds_schema = "eu"
    # use eu bounds not unioned_bounds - use geom_10000 for geom column
    bounds_table = "bounds"
    bounds_geom_col = "geom"  # 2000 and 10000 retrieved internally
    bounds_fid_col = "fid"
    target_schema = "metrics"
    target_nodes_table = "segment_metrics"
    target_blocks_table = "blocks"
    target_bldgs_table = "buildings"
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
            core_function=generate_metrics,
            func_args=[
                bound_fid,
                bounds_fid_col,
                bounds_table,
                target_schema,
                target_nodes_table,
                target_blocks_table,
                target_bldgs_table,
            ],
            content_schema=target_schema,
            content_tables=[
                target_nodes_table,
                target_blocks_table,
                target_bldgs_table,
            ],
            bounds_schema=bounds_schema,
            bounds_table=bounds_table,
            bounds_geom_col=bounds_geom_col,
            bounds_fid_col=bounds_fid_col,
            drop=drop,
        )


if __name__ == "__main__":
    """
    Examples are run from the project folder (the folder containing src)
    python -m src.processing.generate_metrics all
    """

    if True:
        parser = argparse.ArgumentParser(description="Compute metrics.")
        parser.add_argument(
            "bounds_fid",
            type=tools.bounds_fid_type,
            help=("A bounds fid as int to load a specific bounds. Use 'all' to load all bounds."),
        )
        parser.add_argument("--drop", action="store_true", help="Whether to drop existing tables.")
        args = parser.parse_args()
        compute_metrics(
            args.bounds_fid,
            drop=args.drop,
        )
    else:
        bounds_fids = [636]
        compute_metrics(
            bounds_fids,
            drop=True,
        )
