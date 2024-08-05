""" """

from __future__ import annotations

import argparse
import json
import logging
import os
import random
import subprocess
import time
import warnings
from pathlib import Path
from typing import Any, Callable

import geopandas as gpd
import networkx as nx
import pandas as pd
import psycopg2
import sqlalchemy
from cityseer.tools import io
from dotenv import load_dotenv
from osgeo import ogr
from shapely import geometry, ops, wkb
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
    for key in ["host", "port", "user", "database", "password"]:
        if key not in db_config:
            raise ValueError(f"Unable to find expected key: {key} in DB_CONFIG")
    return db_config


def get_sqlalchemy_engine() -> sqlalchemy.engine.Engine:
    """ """
    db_config = get_db_config()
    db_con_str = (
        f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}"
        f"@{db_config['host']}:{db_config['port']}/{db_config['database']}"
    )
    return sqlalchemy.create_engine(db_con_str, pool_pre_ping=True)


def db_execute(query: str, params: tuple[Any] | None = None) -> None:
    """ """
    with psycopg2.connect(**get_db_config()) as db_con:
        with db_con.cursor() as cursor:
            cursor.execute(query, params)
            db_con.commit()


def db_fetch(query: str, params: tuple[Any] | None = None) -> Any:
    """ """
    with psycopg2.connect(**get_db_config()) as db_con:
        with db_con.cursor() as cursor:
            cursor.execute(query, params)
            rows = cursor.fetchall()
    return rows


Connector = tuple[str, geometry.Point]


def split_street_segment(
    line_string: geometry.LineString, connector_infos: list[Connector]
) -> list[tuple[str, str, geometry.LineString]]:
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
        # if only two connectors, check that these are endpoints and continue
        if len(new_connectors) == 2:
            node_segment_pairs.append((old_line_string, *new_connectors))
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
            node_segment_lots.append((line_string_a, new_connectors))
            node_segment_lots.append((line_string_b, new_connectors))
            break
    return node_segment_pairs


def generate_graph(
    nodes_gdf: gpd.GeoDataFrame,
    edges_gdf: gpd.GeoDataFrame,
    road_class_col: str | None = None,
    drop_road_classes: list[str] = ["motorway", "parkingAisle"],
) -> nx.MultiGraph:
    """ """
    logger.info("Preparing GeoDataFrames")
    # create graph
    multigraph = nx.MultiGraph()
    # filter by boundary and build nx
    logger.info("Adding nodes to graph")
    # dedupe nodes
    node_map = {}
    for node_row in tqdm(nodes_gdf.itertuples()):
        # catch duplicates in case of overture dupes by xy or database dupes
        xy_key = f"{node_row.geom.x}-{node_row.geom.y}"
        if xy_key not in node_map:
            node_map[xy_key] = node_row.Index
        # merged key
        merged_key = node_map[xy_key]
        # only insert if new
        if not multigraph.has_node(node_row.Index):
            multigraph.add_node(
                merged_key,
                x=node_row.geom.x,
                y=node_row.geom.y,
            )
    logger.info("Adding edges to graph")
    kept_road_types: set[str] = set()
    for edge_idx, edges_data in tqdm(edges_gdf.iterrows()):
        if road_class_col is not None:
            if edges_data[road_class_col] in drop_road_classes:
                continue
            kept_road_types.add(edges_data[road_class_col])
        # drop tunnels
        road_data = json.loads(edges_data["road"])
        if "flags" in road_data:
            if "isTunnel" in road_data["flags"]:
                continue
        uniq_fids = set()
        connector_fids: list[str] = json.loads(edges_data.connectors)
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
                for edge_idx, edge_val in dict(edges).items():
                    if edge_val["geom"].buffer(1).contains(seg_geom):
                        dupe = True
                        break
            if dupe is False:
                multigraph.add_edge(
                    node_info_a[0],
                    node_info_b[0],
                    edge_idx=edge_idx,
                    level=edges_data.level,
                    geom=seg_geom,
                )
    logger.info(f'Dropped road types: {", ".join(drop_road_classes)}')
    logger.info(f'Kept road types: {", ".join(kept_road_types)}')

    return multigraph


def generate_overture_schema() -> dict[str, list[str]]:
    """ """
    logger.info("Preparing Overture schema")
    overture_csv_file_path = "./src/raw_landuse_schema.csv"
    schema = {
        "eat_and_drink": [],
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
                if not "[" in splits[1]:
                    logger.info(f"Skipping line {line}")
                    continue
                cats = splits[1].strip("\n[]")
                cats = cats.split(",")
                if category in cats:
                    schema[category].append(splits[0])
    return schema


def snip_overture_by_extents(
    path: str | Path,
    bounds_buff: geometry.Polygon,
) -> gpd.GeoDataFrame:
    """ """
    # prepare paths
    input_path = Path(path)
    if not str(input_path).endswith("gpkg") and not str(input_path).endswith("parquet"):
        raise ValueError(f'Expected file path to end with "gpkg" or "geo/parquet": {input_path}')
    if not input_path.exists():
        raise ValueError(f"Input path does not exist: {input_path}")
    # TODO: pending issue https://github.com/OvertureMaps/overturemaps-py/issues/40
    # for now importing with a locally modified script which injects covering field metadata for bbox
    gdf = gpd.read_parquet(str(input_path.resolve()), bbox=bounds_buff.bounds)

    return gdf


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
            CREATE INDEX IF NOT EXISTS bounds_fid_idx ON {target_db_schema}.{target_db_table} (bounds_fid);
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
        raise IOError(f"Cannot proceed because the {bounds_schema}.{bounds_table} table does not exist.")
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


def create_file_spatial_index(file_path: str | Path) -> None:
    """ """
    file_ds = ogr.Open(file_path, update=True)
    if file_ds:
        num_layers = file_ds.GetLayerCount()
        for i in range(num_layers):
            layer = file_ds.GetLayerByIndex(i)
            layer_name = layer.GetName()
            geom_col_name = layer.GetGeometryColumn()
            if not geom_col_name:
                continue
            try:
                subprocess.run(
                    ["ogrinfo", "-sql", f"SELECT CreateSpatialIndex('{layer_name}', '{geom_col_name}')", file_path],
                    check=True,  # Check for command success
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,  # Capture output as text
                )
            except subprocess.CalledProcessError as err:
                logger.error(err.stdout)
                logger.error(err.stderr)
                raise err

        # Close the GeoPackage
        file_ds = None


def load_bounds_fid_network_from_db(engine: sqlalchemy.Engine, bounds_fid: int, buffer_col: str) -> nx.MultiGraph:
    """ """
    # load nodes
    nodes_gdf: gpd.GeoDataFrame = gpd.read_postgis(  # type: ignore
        f"""
        SELECT
            c.fid,
            c.ns_node_idx,
            c.x,
            c.y,
            ST_Contains(b.geom, c.geom) as live,
            c.weight,
            c.geom
        FROM overture.network_nodes_clean c, eu.bounds b
            WHERE b.fid = {bounds_fid}
                AND ST_Contains(b.{buffer_col}, c.geom)
        """,
        engine,
        index_col="fid",
        geom_col="geom",
    )
    # load edges
    edges_gdf: gpd.GeoDataFrame = gpd.read_postgis(  # type: ignore
        f"""
        SELECT
            c.fid,
            c.ns_edge_idx,
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
            c.total_bearing,
            c.geom
        FROM overture.network_edges_clean c, eu.bounds b
            WHERE b.fid = {bounds_fid}
                AND ST_Contains(b.{buffer_col}, c.geom)
        """,
        engine,
        index_col="fid",
        geom_col="geom",
    )
    logger.info("Generating networkx graph")
    multigraph = io.nx_from_cityseer_geopandas(nodes_gdf, edges_gdf)

    return multigraph


def bounds_fid_type(value):
    if value == "all":
        return value
    try:
        return [int(value)]
    except ValueError:
        raise argparse.ArgumentTypeError(f"{value} is not a valid bounds_fid. It must be an integer or 'all'.")
