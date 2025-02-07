""" """

import argparse
import json
import logging
import os
import random
import time
import warnings
from collections.abc import Callable
from typing import Any, cast

import geopandas as gpd
import networkx as nx
import numpy as np
import pandas as pd
import psycopg
import sqlalchemy
from cityseer.tools import io
from dotenv import load_dotenv
from pyproj import Transformer
from shapely import geometry, ops, wkb
from shapely.ops import transform
from tqdm import tqdm

warnings.simplefilter(action="ignore", category=pd.errors.PerformanceWarning)


def get_logger(name: str, log_level: int = logging.INFO) -> logging.Logger:
    logging.basicConfig(level=log_level)
    return logging.getLogger(name)


logger = get_logger(__name__)

load_dotenv()


def get_db_config() -> dict[str, str | None]:
    """ """
    db_config_json = os.getenv("DB_CONFIG")
    if db_config_json is None:
        raise ValueError("Unable to retrieve DB_CONFIG environment variable.")
    db_config = json.loads(db_config_json)
    for key in ["host", "port", "user", "dbname", "password"]:
        if key not in db_config:
            raise ValueError(f"Unable to find expected key: {key} in DB_CONFIG")
    return db_config


def get_sqlalchemy_engine() -> sqlalchemy.engine.Engine:
    """ """
    db_config = get_db_config()
    db_con_str = (
        f"postgresql+psycopg://{db_config['user']}:{db_config['password']}"
        f"@{db_config['host']}:{db_config['port']}/{db_config['dbname']}"
    )
    return sqlalchemy.create_engine(db_con_str, pool_pre_ping=True)


def db_execute(query: str, params: tuple[Any] | None = None) -> None:
    """ """
    with psycopg.connect(**get_db_config()) as db_con, db_con.cursor() as cursor:  # type: ignore
        cursor.execute(query, params)  # type: ignore
        db_con.commit()


def db_fetch(query: str, params: tuple[Any] | None = None) -> Any:
    """ """
    with psycopg.connect(**get_db_config()) as db_con, db_con.cursor() as cursor:  # type: ignore
        cursor.execute(query, params)  # type: ignore
        rows = cursor.fetchall()
    return rows


def convert_ndarrays(obj: Any) -> Any:
    if isinstance(obj, np.ndarray):
        return convert_ndarrays(obj.tolist())
    if isinstance(obj, list | tuple):
        return [convert_ndarrays(item) for item in obj]
    if isinstance(obj, dict):
        return {key: convert_ndarrays(value) for key, value in obj.items()}
    if obj is None or obj == "":
        return None
    if isinstance(obj, str | int | float):
        return obj
    raise ValueError(f"Unhandled type when converting: {type(obj).__name__}")


def col_to_json(obj: Any) -> str | None:
    """Extracts JSON from a geoparquet / geopandas column"""
    if obj is None or (isinstance(obj, str) and obj == ""):
        return "null"
    obj = convert_ndarrays(obj)
    return json.dumps(obj)


Connector = tuple[str, geometry.Point]


def split_street_segment(
    line_string: geometry.LineString, connector_infos: list[Connector]
) -> list[tuple[geometry.LineString, Connector, Connector]]:
    """ """
    # overture segments can span multiple intersections
    # sort through and split until pairings are ready for insertion to the graph
    node_segment_pairs: list[tuple[geometry.LineString, Connector, Connector]] = []
    node_segment_lots: list[tuple[geometry.LineString, list[Connector]]] = [(line_string, connector_infos)]
    # start iterating
    while node_segment_lots:
        old_line_string, old_connectors = node_segment_lots.pop()
        # filter down connectors
        new_connectors: list[tuple[str, geometry.Point]] = []
        # if the point doesn't touch the line, discard
        for _fid, _point in old_connectors:
            if _point.distance(old_line_string) > 0:
                continue
            new_connectors.append((_fid, _point))
        # if only two connectors
        if len(new_connectors) == 2:
            node_segment_pairs.append((old_line_string, new_connectors[0], new_connectors[1]))
            continue
        # look for splits
        for _fid, _point in new_connectors:
            splits = ops.split(old_line_string, _point)
            # continue if an endpoint
            if len(splits.geoms) == 1:
                continue
            # otherwise unpack
            line_string_a, line_string_b = splits.geoms
            # otherwise split into two bundles and reset
            node_segment_lots.append((cast(geometry.LineString, line_string_a), new_connectors))
            node_segment_lots.append((cast(geometry.LineString, line_string_b), new_connectors))
            break
    return node_segment_pairs


def generate_graph(
    nodes_gdf: gpd.GeoDataFrame,
    edges_gdf: gpd.GeoDataFrame,
    drop_road_types: list[str] | None = None,
) -> nx.MultiGraph:
    """ """
    if drop_road_types is None:
        drop_road_types = []
    logger.info("Preparing GeoDataFrames")
    # create graph
    multigraph = nx.MultiGraph()
    # filter by boundary and build nx
    logger.info("Adding nodes to graph")
    # dedupe nodes
    node_map = {}
    for node_row in tqdm(nodes_gdf.itertuples(), total=len(nodes_gdf)):
        # catch duplicates in case of overture dupes by xy or database dupes
        x = node_row.geom.x  # type: ignore
        y = node_row.geom.y  # type: ignore
        xy_key = f"{x}-{y}"
        if xy_key not in node_map:
            node_map[xy_key] = node_row.Index
        # merged key
        merged_key = node_map[xy_key]
        # only insert if new
        if not multigraph.has_node(node_row.Index):
            multigraph.add_node(
                merged_key,
                x=x,
                y=y,
            )
    logger.info("Adding edges to graph")
    dropped_road_types = set()
    kept_road_types = set()
    for edge_idx, edges_data in tqdm(edges_gdf.iterrows(), total=len(edges_gdf)):
        road_class = edges_data["class"]
        if road_class in drop_road_types:
            dropped_road_types.add(road_class)
            continue
        kept_road_types.add(road_class)
        uniq_fids = set()
        connector_fids: list[str] = [connector["connector_id"] for connector in edges_data["connectors"]]
        connector_infos: list[tuple[str, geometry.Point]] = []
        missing_connectors = False
        for connector_fid in connector_fids:
            # skip malformed edges - this happens at boundary thresholds with missing nodes in relation to edges
            if connector_fid not in multigraph:
                missing_connectors = True
                break
            # deduplicate
            x, y = multigraph.nodes[connector_fid]["x"], multigraph.nodes[connector_fid]["y"]
            xy_key = f"{x}-{y}"
            merged_key = node_map[xy_key]
            if merged_key in uniq_fids:
                continue
            uniq_fids.add(merged_key)
            # track
            connector_point = geometry.Point(x, y)
            connector_infos.append((merged_key, connector_point))
        if missing_connectors is True:
            continue
        if len(connector_infos) < 2:
            # logger.warning("Only one connector pair for edge")
            continue
        # extract levels, names, routes, highways
        # do this once instead of for each new split segment
        levels = set([])
        if edges_data["level_rules"] is not None:
            for level_info in edges_data["level_rules"]:
                levels.add(level_info["value"])
        names = []  # takes list form for nx
        if edges_data["names"] is not None and "primary" in edges_data["names"]:
            names.append(edges_data["names"]["primary"])
        routes = set([])
        if edges_data["routes"] is not None:
            for routes_info in edges_data["routes"]:
                if "ref" in routes_info:
                    routes.add(routes_info["ref"])
        is_tunnel = False
        is_bridge = False
        if edges_data["road_flags"] is not None:
            for flags_info in edges_data["road_flags"]:
                if "is_tunnel" in flags_info["values"]:
                    is_tunnel = True
                if "is_bridge" in flags_info["values"]:
                    is_bridge = True
        highways = []  # takes list form for nx
        if road_class is not None and road_class not in ["unknown"]:
            highways.append(road_class)
        # split segments and build
        street_segs = split_street_segment(edges_data.geom, connector_infos)
        for seg_geom, node_info_a, node_info_b in street_segs:
            if not node_info_a[1].touches(seg_geom) or not node_info_b[1].touches(seg_geom):
                raise ValueError(
                    "Edge and endpoint connector are not touching. "
                    f"See connectors: {node_info_a[0]} and {node_info_b[0]}"
                )
            # don't add duplicates
            dupe = False
            if multigraph.has_edge(node_info_a[0], node_info_b[0]):
                edges = multigraph[node_info_a[0]][node_info_b[0]]
                for _edge_idx, edge_val in dict(edges).items():
                    if edge_val["geom"].buffer(1).contains(seg_geom):
                        dupe = True
                        break
            if dupe is False:
                multigraph.add_edge(
                    node_info_a[0],
                    node_info_b[0],
                    edge_idx=edge_idx,
                    geom=seg_geom,
                    levels=list(levels),
                    names=names,
                    routes=list(routes),
                    highways=highways,
                    is_bridge=is_bridge,
                    is_tunnel=is_tunnel,
                )
    logger.info(f"Dropped road types: {', '.join(dropped_road_types)}")
    logger.info(f"Kept road types: {', '.join(kept_road_types)}")

    return multigraph


def generate_overture_schema() -> dict[str, list[str]]:
    """ """
    logger.info("Preparing Overture schema")
    overture_csv_file_path = "./src/raw_landuse_schema.csv"
    schema = {
        # "eat_and_drink": [], - don't use because places overriden by more specific categories
        "restaurant": [],
        "bar": [],
        "cafe": [],
        "accommodation": [],
        "automotive": [],
        "arts_and_entertainment": [],
        "attractions_and_activities": [],
        "active_life": [],
        "beauty_and_spa": [],
        "education": [],
        "financial_service": [],
        "private_establishments_and_corporates": [],
        "retail": [],
        "health_and_medical": [],
        "pets": [],
        "business_to_business": [],
        "public_service_and_government": [],
        "religious_organization": [],
        "real_estate": [],
        "travel": [],
        "mass_media": [],
        "home_service": [],
        "professional_services": [],
        "structure_and_geography": [],
    }
    for category, _list_val in schema.items():
        with open(overture_csv_file_path) as schema_csv:
            logger.info(f"Gathering category: {category}")
            for line in schema_csv:
                # remove header line
                if "Overture Taxonomy" in line:
                    continue
                splits = line.split(";")
                if "[" not in splits[1]:
                    logger.info(f"Skipping line {line}")
                    continue
                cats = splits[1].strip("\n[]")
                cats = cats.split(",")
                if category in cats:
                    schema[category].append(splits[0])
    return schema


def iter_boundaries(
    db_schema: str, db_table: str, fid_col: int | str, geom_col: str, wgs84: bool
) -> list[tuple[int | str, geometry.Polygon]]:
    """ """
    if wgs84:
        bound_records = db_fetch(
            f"""
            SELECT {fid_col} as fid, ST_Transform({geom_col}, 4326) as geom
            FROM {db_schema}.{db_table}
            ORDER BY fid DESC
            """
        )
    else:
        bound_records = db_fetch(
            f"""
            SELECT {fid_col} as fid, {geom_col} as geom
            FROM {db_schema}.{db_table}
            ORDER BY fid
            """
        )
    boundaries = []
    for bound_record in bound_records:
        boundaries.append((bound_record[0], wkb.loads(bound_record[1], hex=True)))
    return boundaries


def prepare_schema(overture_schema_name: str):
    """ """
    logger.info(f"Creating schema {overture_schema_name} if necessary.")
    db_execute(
        f"""
        CREATE SCHEMA IF NOT EXISTS {overture_schema_name};
        """
    )


def check_table_exists(db_schema: str, db_table: str) -> bool:
    """ """
    exists = db_fetch(
        f"""
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = '{db_schema}' AND table_name = '{db_table}'
        );
        """
    )[0][0]
    logger.info(f"Checking if table {db_schema}.{db_table} exists: {exists}.")
    return bool(exists)


def init_tracking_table(
    load_key: str, template_bounds_schema: str, template_bounds_table: str, fid_col: int | str, geom_col: str
) -> None:
    """ """
    if not check_table_exists("loads", load_key):
        db_execute(
            f"""
            CREATE SCHEMA IF NOT EXISTS loads;
            CREATE TABLE IF NOT EXISTS loads.{load_key}
            AS SELECT
                {fid_col} as fid,
                False as loaded,
                {geom_col} as geom
            FROM {template_bounds_schema}.{template_bounds_table};
            """
        )
        logger.info(
            f"Created loading extents tracking table loads.{load_key} "
            f"using bounds {template_bounds_schema}.{template_bounds_table} as template"
        )


def tracking_state_reset_loaded(load_key: str, bounds_fid: int | str) -> None:
    """ """
    logger.info(f"Setting loaded state to False for bounds fid {bounds_fid}.")
    db_execute(
        f"""
        UPDATE loads.{load_key}
        SET loaded = false
        WHERE fid = {bounds_fid} 
        """
    )


def tracking_state_check_loaded(load_key: str, bounds_fid: int | str) -> bool:
    loaded = db_fetch(
        f"""
        SELECT loaded
        FROM loads.{load_key}
        WHERE fid = {bounds_fid};
    """
    )[0][0]
    logger.info(f"Checking if bounds fid {bounds_fid} is loaded: {loaded}.")
    return bool(loaded)


def tracking_state_set_loaded(load_key: str, bounds_fid: int | str):
    """ """
    logger.info(f"Setting loaded state to True for bounds fid {bounds_fid}.")
    db_execute(
        f"""
        UPDATE loads.{load_key}
        SET loaded = true
        WHERE fid = {bounds_fid}
        """
    )


def drop_table(target_db_schema: str, target_db_table: str) -> None:
    """ """
    if check_table_exists(target_db_schema, target_db_table):
        logger.warning(f"Dropping table {target_db_schema}.{target_db_table}")
        db_execute(
            f"""
            DROP TABLE {target_db_schema}.{target_db_table};
            """
        )


def drop_content(
    target_db_schema: str,
    target_db_table: str,
    bounds_schema: str,
    bounds_table: str,
    bounds_fid: int | str,
) -> None:
    """ """
    if check_table_exists(target_db_schema, target_db_table):
        logger.info(
            f"Dropping content from {target_db_schema}.{target_db_table} "
            f"where intersecting {bounds_schema}.{bounds_table} for bounds {bounds_fid}"
        )
        # drop_content has an expectation that a bounds_fid column exists
        # how to make this more explicit in future regarding upstream workflows
        db_execute(
            f"""
            CREATE INDEX IF NOT EXISTS idx_{target_db_schema}_{target_db_table}_bounds_fid 
                ON {target_db_schema}.{target_db_table} (bounds_fid);
            """
        )
        db_execute(
            f"""
            DELETE FROM {target_db_schema}.{target_db_table} AS target
                WHERE bounds_fid = {bounds_fid};
            """
        )


def process_func_with_bound_tracking(
    bound_fid: int | str,
    load_key: str,
    core_function: Callable,
    func_args: list,
    content_schema: str,
    content_tables: list[str],
    bounds_schema: str,
    bounds_table: str,
    bounds_geom_col: str,
    bounds_fid_col: str,
    drop=False,
):
    """ """
    # random sleep to prevent races / locks when running in multiprocessing
    time.sleep(random.uniform(0.5, 1.0))
    # check if bounds table exists
    if not check_table_exists(bounds_schema, bounds_table):
        raise OSError(f"Cannot proceed because the {bounds_schema}.{bounds_table} table does not exist.")
    # check that tracking table is initiated
    init_tracking_table(load_key, bounds_schema, bounds_table, bounds_fid_col, bounds_geom_col)
    # check if loaded
    loaded = tracking_state_check_loaded(load_key, bound_fid)
    if drop is True or not loaded:
        # clear out even if drop is not True so that partially loaded content is cleared
        for content_table in content_tables:
            drop_content(content_schema, content_table, bounds_schema, bounds_table, bound_fid)
        tracking_state_reset_loaded(load_key, bound_fid)
        logger.info(f"Loading {bounds_schema}.{bounds_table} bounds fid {bound_fid}")
        core_function(*func_args)
        tracking_state_set_loaded(load_key, bound_fid)


def load_bounds_fid_network_from_db(
    engine: sqlalchemy.Engine, bounds_fid: int, buffer_col: str
) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame, Any]:
    """ """
    logger.info("Loading nodes")
    # load nodes - i.e. where primal node (centroid of dual segment) is contained
    nodes_gdf: gpd.GeoDataFrame = gpd.read_postgis(  # type: ignore
        f"""
        SELECT
            c.fid,
            c.x,
            c.y,
            ST_Contains(b.geom, ST_Centroid(c.primal_edge)) as live,
            c.weight,
            c.primal_edge as geom
        FROM overture.dual_nodes c, eu.bounds b
            WHERE b.fid = {bounds_fid}
                AND ST_Intersects(b.{buffer_col}, c.primal_edge)
                AND ST_Contains(b.{buffer_col}, ST_Centroid(c.primal_edge))
        """,
        engine,
        index_col="fid",
        geom_col="geom",
    )
    logger.info("Loading edges")
    # load edges where contained - i.e. to connect loaded nodes
    edges_gdf: gpd.GeoDataFrame = gpd.read_postgis(  # type: ignore
        f"""
        SELECT
            c.fid,
            c.start_ns_node_idx,
            c.end_ns_node_idx,
            c.edge_idx,
            c.nx_start_node_key,
            c.nx_end_node_key,
            c.length,
            c.angle_sum,
            c.imp_factor,
            c.in_bearing,
            c.out_bearing,
            c.geom
        FROM overture.dual_edges c, eu.bounds b
            WHERE b.fid = {bounds_fid}
                AND ST_Contains(b.{buffer_col}, c.geom)
        """,
        engine,
        index_col="fid",
        geom_col="geom",
    )
    if len(nodes_gdf) == 0:
        raise OSError(f"No network data for bounds FID: {bounds_fid}")
    logger.info("Building network structure")
    network_structure = io.network_structure_from_gpd(nodes_gdf, edges_gdf)

    return nodes_gdf, edges_gdf, network_structure


def bounds_fid_type(value):
    if value == "all":
        return value
    try:
        return [int(value)]
    except ValueError as e:
        raise argparse.ArgumentTypeError(f"{value} is not a valid bounds_fid. It must be an integer or 'all'.") from e


def reproject_geometry(geom, from_crs, to_crs):
    """ """
    transformer = Transformer.from_crs(from_crs, to_crs, always_xy=True)
    reprojected_geom = transform(transformer.transform, geom)

    return reprojected_geom
