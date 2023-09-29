""" """
import asyncio
import json
import logging
import os
import warnings
from typing import Any

import networkx as nx
import pandas as pd
import psycopg2
import sqlalchemy
from dotenv import load_dotenv  # type: ignore
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
    return sqlalchemy.create_engine(db_con_str)


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


async def postgis_to_nx(
    nodes_db_schema: str,
    nodes_db_table: str,
    edges_db_schema: str,
    edges_db_table: str,
    boundary_id: str,
):
    """ """
    logger = get_logger(__name__)
    logger.info(f"Generating graph from database for city uid: {city_pop_id}")
    multigraph = nx.MultiGraph()
    logger.info("Loading nodes data")
    db_con = await get_db_con()
    node_records: Any = await db_con.fetch(
        f"""
        SELECT uid, within, geom
        FROM {nodes_db_schema}.{nodes_db_table}
        WHERE boundary_id = {boundary_id}"""
    )
    for node_data in tqdm(node_records):
        geom: geometry.Point = wkb.loads(node_data["geom"], hex=True)  # type: ignore
        multigraph.add_node(node_data["uid"], x=geom.x, y=geom.y, live=node_data["within"])  # pylint: disable=no-member
    logger.info("Loading edges data")
    edge_records: Any = await db_con.fetch(
        f"""
        SELECT uid, node_a, node_b, geom
        FROM {edges_db_schema}.{edges_db_table}
        WHERE boundary_id = {boundary_id}"""
    )
    await db_con.close()
    # handle (literal) edge cases
    for edge_data in tqdm(edge_records):
        if edge_data["node_a"] in multigraph and edge_data["node_b"] in multigraph:
            multigraph.add_edge(
                edge_data["node_a"],
                edge_data["node_b"],
                geom=wkb.loads(edge_data["geom"], hex=True),
            )

    return multigraph


async def _iter_boundaries(
    boundaries_schema: str, boundaries_table: str, start: int, end: int, buffer: int
) -> list[tuple[int, geometry.Polygon, geometry.Polygon]]:
    """ """
    db_con = await get_db_con()
    response: list[Any] = await db_con.fetch(
        f"""
    SELECT pop_id, geom as bound_geom, ST_Simplify(ST_ConvexHull(ST_Buffer(geom, $1)), 20) as convex_buff_geom
    FROM {boundaries_schema}.{boundaries_table}
    WHERE pop_id >= {start} AND pop_id <= {end}
    ORDER BY pop_id;
    """,
        buffer,
    )
    rows = [
        (
            row["pop_id"],
            wkb.loads(row["bound_geom"], hex=True),
            wkb.loads(row["convex_buff_geom"], hex=True),
        )
        for row in response
    ]
    return rows  # type: ignore


def iter_boundaries(
    boundaries_schema: str,
    boundaries_table: str,
    start: int = 1,
    end: int = 2,
    buffer: int = 2000,
) -> list[tuple[int, geometry.Polygon, geometry.Polygon]]:
    """ """
    rows: list[tuple[int, geometry.Polygon]] = asyncio.run(
        _iter_boundaries(boundaries_schema, boundaries_table, start, end, buffer)  # type: ignore
    )
    return rows  # type: ignore
