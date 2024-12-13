""" """

import argparse

import geopandas as gpd
from cityseer.metrics import layers
from tqdm import tqdm

from src import tools

logger = tools.get_logger(__name__)

OVERTURE_SCHEMA = tools.generate_overture_schema()


def process_landuses(
    bounds_fid: int,
    bounds_fid_col: str,
    bounds_geom_col: str,
    bounds_table: str,
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
    # POI
    places_gdf = gpd.read_postgis(
        f"""
        SELECT p.fid, p.main_cat, p.geom
        FROM overture.overture_place p, eu.{bounds_table} b
        WHERE b.{bounds_fid_col} = {bounds_fid}
            AND ST_Intersects(b.{bounds_geom_col}, p.geom)
        """,
        engine,
        index_col="fid",
        geom_col="geom",
    )
    # prepare keys
    landuse_keys = list(OVERTURE_SCHEMA.keys())
    # remove structure and geography category
    landuse_keys.remove("structure_and_geography")
    places_gdf = places_gdf[places_gdf["main_cat"] != "structure_and_geography"]  # type: ignore
    # remove mass_media category
    landuse_keys.remove("mass_media")
    places_gdf = places_gdf[places_gdf["main_cat"] != "mass_media"]  # type: ignore
    # compute accessibilities
    nodes_gdf, places_gdf = layers.compute_accessibilities(
        places_gdf,  # type: ignore
        landuse_column_label="main_cat",
        accessibility_keys=landuse_keys,
        nodes_gdf=nodes_gdf,
        network_structure=network_structure,
        distances=[100, 500, 1500],
    )
    nodes_gdf, places_gdf = layers.compute_mixed_uses(
        places_gdf,
        landuse_column_label="main_cat",
        nodes_gdf=nodes_gdf,
        network_structure=network_structure,
        distances=[100, 500, 1500],
    )
    # infrastructure
    street_furn_keys = [
        "bench",
        "drinking_water",
        "fountain",
        "picnic_table",
        "plant",
        "planter",
        "post_box",
    ]
    parking_keys = [
        # "bicycle_parking",
        "motorcycle_parking",
        "parking",
    ]
    transport_keys = [
        "aerialway_station",
        "airport",
        "bus_station",
        "bus_stop",
        "ferry_terminal",
        "helipad",
        "international_airport",
        "railway_station",
        "regional_airport",
        "seaplane_airport",
        "subway_station",
    ]
    infrast_gdf = gpd.read_postgis(
        f"""
        SELECT p.fid, p.class, p.geom
        FROM overture.overture_infrast p, eu.{bounds_table} b
        WHERE b.{bounds_fid_col} = {bounds_fid}
            AND ST_Contains(b.{bounds_geom_col}, p.geom)
        """,
        engine,
        index_col="fid",
        geom_col="geom",
    )
    infrast_gdf["class"] = infrast_gdf["class"].replace(street_furn_keys, "street_furn")  # type: ignore
    infrast_gdf["class"] = infrast_gdf["class"].replace(parking_keys, "parking")  # type: ignore
    infrast_gdf["class"] = infrast_gdf["class"].replace(transport_keys, "transport")  # type: ignore
    landuse_keys = ["street_furn", "parking", "transport"]
    infrast_gdf = infrast_gdf[infrast_gdf["class"].isin(landuse_keys)]  # type: ignore
    # compute accessibilities
    nodes_gdf, infrast_gdf = layers.compute_accessibilities(
        infrast_gdf,  # type: ignore
        landuse_column_label="class",
        accessibility_keys=landuse_keys,
        nodes_gdf=nodes_gdf,
        network_structure=network_structure,
        distances=[100, 500, 1500],
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


def compute_landuse_metrics(
    target_bounds_fids: list[int] | str,
    drop: bool = False,
):
    if not (tools.check_table_exists("overture", "dual_nodes") and tools.check_table_exists("overture", "dual_edges")):
        raise OSError("The cleaned network nodes and edges tables need to be created prior to proceeding.")
    logger.info("Computing landuse metrics")
    tools.prepare_schema("metrics")
    load_key = "metrics_landuses"
    bounds_schema = "eu"
    # use eu bounds not unioned_bounds - use geom_2000 for geom column
    bounds_table = "bounds"
    bounds_geom_col = "geom_2000"
    bounds_fid_col = "fid"
    target_schema = "metrics"
    target_table = "landuses"
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
            core_function=process_landuses,
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
    python -m src.processing.metrics_landuses all
    """

    if True:
        parser = argparse.ArgumentParser(description="Compute landuse metrics.")
        parser.add_argument(
            "bounds_fid",
            type=tools.bounds_fid_type,
            help=("A bounds fid as int to load a specific bounds. Use 'all' to load all bounds."),
        )
        parser.add_argument("--drop", action="store_true", help="Whether to drop existing tables.")
        args = parser.parse_args()
        compute_landuse_metrics(
            args.bounds_fid,
            drop=args.drop,
        )
    else:
        bounds_fids = [636]
        compute_landuse_metrics(
            bounds_fids,
            drop=True,
        )
