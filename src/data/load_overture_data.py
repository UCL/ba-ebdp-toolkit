# pyright: basic
"""
DuckDB doesn't set projection so set manually; so, if wanted, these need to be set manually with `ogr2ogr`, for example:

```bash
ogr2ogr -progress -a_srs EPSG:4326 temp/places_4326.gpkg temp/places.gpkg
```

`ogr2ogr` can likewise be used if clipping the datasets, for example:

```bash
ogr2ogr -progress -spat -12.421470912798974 33.226730183416954 45.535158355759435 71.13547646352613 temp/places_eu.gpkg temp/places_4326.gpkg
```

"""
import argparse
import json
import os
import shutil
import subprocess
import time
from pathlib import Path

import geopandas as gpd
from shapely import geometry, wkb
from tqdm import tqdm
from workflows import landuse_schema_overture

from src import tools

logger = tools.get_logger(__name__)
engine = tools.get_sqlalchemy_engine()


def fetch_unioned_extents_4326(
    buffer_col: str, bounds_schema: str, bounds_table: str, bounds_fids: list[int]
) -> list[str]:
    """ """
    # start with network layers - use union because of overlaps
    bounds_buffs = tools.db_fetch(
        f"""
        WITH union_geoms AS (
            SELECT (ST_Dump(ST_Union({buffer_col}))).geom
            FROM {bounds_schema}.{bounds_table}
            WHERE fid in %s
        )
        SELECT ST_Envelope(ST_Transform(geom, 4326))
        FROM union_geoms
        """,
        (tuple(bounds_fids),),
    )
    return [b[0] for b in bounds_buffs]


def snip_extents(
    path: str,
    bounds_buff: geometry.Polygon,
    path_key: str,
    bin_path: str | None = None,
) -> gpd.GeoDataFrame:
    """ """
    # prepare paths
    input_path = Path(path)
    if not str(input_path).endswith(".gpkg"):
        raise ValueError(f'Expected file with extension of ".gpkg": {input_path}')
    if not input_path.exists():
        raise ValueError(f"Input path does not exist: {input_path}")
    staging_dir = input_path.parent / f"temp_snip_{path_key}"
    if staging_dir.exists():
        shutil.rmtree(staging_dir)
    os.makedirs(staging_dir)
    snip_path = staging_dir / f"snip_{input_path.name}"
    # snip
    attempts = 3
    while attempts > 0:
        attempts -= 1
        try:
            subprocess.run(
                [  # type: ignore
                    "ogr2ogr" if bin_path is None else str(Path(bin_path) / "ogr2ogr"),
                    "-spat",
                    str(bounds_buff.bounds[0]),
                    str(bounds_buff.bounds[1]),
                    str(bounds_buff.bounds[2]),
                    str(bounds_buff.bounds[3]),
                    str(snip_path),
                    str(input_path),
                ],
                check=True,
            )
            break
        except Exception as err:
            if attempts > 0:
                logger.error(f"Encountered error with ogr2ogr, reattempting after 1s; {attempts} attempts remaining.")
                time.sleep(1)  # try sleeping to give time for database locks to release
            else:
                raise err
    gdf = gpd.read_file(snip_path)
    # cleanup temp directory
    shutil.rmtree(staging_dir)

    return gdf


def process_extent_network(
    bounds_buff_wkb: str,
    overture_nodes_path: str,
    overture_edges_path: str,
    bounds_schema: str,
    bounds_table: str,
    overture_schema_name: str,
    buffer_col: str,
    bin_path: str | None = None,
):
    """ """
    bounds_geom: geometry.Polygon = wkb.loads(bounds_buff_wkb)  # type: ignore
    # NODES
    nodes_gdf = snip_extents(overture_nodes_path, bounds_geom, "nodes", bin_path)
    nodes_gdf.set_index("id", inplace=True)
    nodes_gdf.rename(columns={"geometry": "geom"}, inplace=True)
    nodes_gdf.set_geometry("geom", inplace=True)
    nodes_gdf.drop(columns=["connectors", "road", "version", "level"], inplace=True)
    nodes_gdf.to_crs(3035).to_postgis(
        "overture_nodes", engine, if_exists="append", schema=overture_schema_name, index=True, index_label="fid"
    )
    tools.db_execute(
        f"""
        DELETE FROM {overture_schema_name}.overture_nodes nd
        WHERE NOT EXISTS (
            SELECT 1 
            FROM {bounds_schema}.{bounds_table} bb
            WHERE ST_Contains({buffer_col}, nd.geom)
        );
                """
    )
    # EDGES
    edges_gdf = snip_extents(overture_edges_path, bounds_geom, "edges", bin_path)
    edges_gdf.set_index("id", inplace=True)
    edges_gdf.rename(columns={"geometry": "geom"}, inplace=True)
    edges_gdf.set_geometry("geom", inplace=True)

    # extract connectors as list of str
    def extract_connectors(json_string):
        connectors = json.loads(json_string)
        connectors = [c for c in connectors]
        return json.dumps(connectors)

    edges_gdf = edges_gdf.rename(columns={"connectors": "_connectors"})
    edges_gdf["connectors"] = edges_gdf["_connectors"].apply(extract_connectors)
    edges_gdf = edges_gdf.drop(columns=["_connectors"])

    # extract class
    def extract_class(json_string):
        road_info = json.loads(json_string)
        if "class" in road_info:
            return road_info["class"]
        return None

    edges_gdf["road_class"] = edges_gdf["road"].apply(extract_class)

    # extract surface
    def extract_surface(json_string):
        road_info = json.loads(json_string)
        if "surface" in road_info:
            return road_info["surface"]
        return None

    edges_gdf["surface"] = edges_gdf["road"].apply(extract_surface)
    edges_gdf.to_crs(3035).to_postgis(
        "overture_edges", engine, if_exists="append", schema=overture_schema_name, index=True, index_label="fid"
    )
    tools.db_execute(
        f"""
        DELETE FROM {overture_schema_name}.overture_edges nd
        WHERE NOT EXISTS (
            SELECT 1 
            FROM {bounds_schema}.{bounds_table} bb
            WHERE ST_Contains({buffer_col}, nd.geom)
        );
                """
    )


def load_overture_networks(
    bounds_fids: list[int],
    overture_nodes_path: str,
    overture_edges_path: str,
    bounds_schema: str,
    bounds_table: str,
    overture_schema_name: str,
    buffer_col: str,
    bin_path: str | None = None,
    overwrite: bool = False,
) -> None:
    """ """
    logger.info("Loading overture networks")
    tools.check_exists(overwrite, overture_schema_name, "overture_nodes")
    tools.check_exists(overwrite, overture_schema_name, "overture_edges")
    tools.prepare_schema(overture_schema_name)
    bounds_buffs = fetch_unioned_extents_4326(buffer_col, bounds_schema, bounds_table, bounds_fids)
    for bounds_row in tqdm(bounds_buffs):
        process_extent_network(
            bounds_row,
            overture_nodes_path,
            overture_edges_path,
            bounds_schema,
            bounds_table,
            overture_schema_name,
            buffer_col,
            bin_path,
        )


def process_extent_places(
    bounds_buff_wkb: str,
    overture_places_path: str,
    bounds_schema: str,
    bounds_table: str,
    overture_schema_name: str,
    buffer_col: str,
    bin_path: str | None = None,
):
    """ """
    bounds_geom: geometry.Polygon = wkb.loads(bounds_buff_wkb)  # type: ignore
    places_gdf = snip_extents(overture_places_path, bounds_geom, "places", bin_path)
    places_gdf.set_index("id", inplace=True)
    places_gdf.rename(columns={"geometry": "geom"}, inplace=True)
    places_gdf.set_geometry("geom", inplace=True)

    def extract_main_cat(json_string: str):
        categories = json.loads(json_string)
        return categories["main"]

    def extract_alt_cat(json_string: str):
        categories = json.loads(json_string)
        return json.dumps(categories["alternate"])

    def extract_name(json_string: str):
        name_info = json.loads(json_string)
        return name_info["common"][0]["value"]

    sift = set()

    def assign_major_cat(desc: str):
        for major_cat, major_cat_vals in landuse_schema_overture.LANDUSE_CATS.items():
            # same parent categories have further sub categories, others not
            if isinstance(major_cat_vals, list):
                if desc in major_cat_vals:
                    return major_cat
            else:
                for minor_cat_vals in major_cat_vals.values():
                    if desc in minor_cat_vals:
                        return major_cat
        if desc is not None:
            sift.add(desc)
        return None

    def assign_sub_cat(desc: str):
        for major_cat, major_cat_vals in landuse_schema_overture.LANDUSE_CATS.items():
            # same parent categories have further sub categories, others not
            if isinstance(major_cat_vals, list):
                if desc in major_cat_vals:
                    return major_cat
            else:
                for minor_cat, minor_cat_vals in major_cat_vals.items():
                    if desc in minor_cat_vals:
                        return minor_cat
        return None

    places_gdf["main_cat"] = places_gdf["categories"].apply(extract_main_cat)
    places_gdf["alt_cat"] = places_gdf["categories"].apply(extract_alt_cat)
    places_gdf["common_name"] = places_gdf["names"].apply(extract_name)
    places_gdf["major_cat"] = places_gdf["main_cat"].apply(assign_major_cat)
    places_gdf["minor_cat"] = places_gdf["main_cat"].apply(assign_sub_cat)
    places_gdf.to_crs(3035).to_postgis(
        "overture_places", engine, if_exists="append", schema=overture_schema_name, index=True, index_label="fid"
    )
    print(f"Items not found: {sift}")
    tools.db_execute(
        f"""
        DELETE FROM {overture_schema_name}.overture_places nd
        WHERE NOT EXISTS (
            SELECT 1 
            FROM {bounds_schema}.{bounds_table} bb
            WHERE ST_Contains({buffer_col}, nd.geom)
        );
                   """
    )


def load_overture_places(
    bounds_fids: list[int],
    overture_places_path: str,
    bounds_schema: str,
    bounds_table: str,
    overture_schema_name: str,
    buffer_col: str,
    bin_path: str | None = None,
    overwrite: bool = False,
) -> None:
    """ """
    logger.info("Loading overture places")
    tools.check_exists(overwrite, overture_schema_name, "overture_places")
    tools.prepare_schema(overture_schema_name)
    bounds_buffs = fetch_unioned_extents_4326(buffer_col, bounds_schema, bounds_table, bounds_fids)
    for bounds_row in tqdm(bounds_buffs):
        process_extent_places(
            bounds_row,
            overture_places_path,
            bounds_schema,
            bounds_table,
            overture_schema_name,
            buffer_col,
            bin_path,
        )


def process_extent_buildings(
    bounds_buff_wkb: str,
    overture_buildings_path: str,
    bounds_schema: str,
    bounds_table: str,
    overture_schema_name: str,
    buffer_col: str,
    bin_path: str | None = None,
):
    # BUILDINGS
    bounds_geom: geometry.Polygon = wkb.loads(bounds_buff_wkb)  # type: ignore
    buildings_gdf = snip_extents(overture_buildings_path, bounds_geom, "bldgs", bin_path)
    buildings_gdf.set_index("id", inplace=True)
    buildings_gdf.rename(columns={"geometry": "geom"}, inplace=True)
    buildings_gdf.set_geometry("geom", inplace=True)

    def ensure_multipolygon(geom):
        if geom.geom_type == "Polygon":
            return geometry.MultiPolygon([geom])
        return geom

    buildings_gdf["geom"] = buildings_gdf["geom"].apply(ensure_multipolygon)
    buildings_gdf.to_crs(3035).to_postgis(
        "overture_buildings", engine, if_exists="append", schema=overture_schema_name, index=True, index_label="fid"
    )
    tools.db_execute(
        f"""
        DELETE FROM {overture_schema_name}.overture_buildings nd
        WHERE NOT EXISTS (
            SELECT 1 
            FROM {bounds_schema}.{bounds_table} bb
            WHERE ST_Contains({buffer_col}, nd.geom)
        );
                   """
    )


def load_overture_buildings(
    bounds_fids: list[int],
    overture_buildings_path: str,
    bounds_schema: str,
    bounds_table: str,
    overture_schema_name: str,
    buffer_col: str,
    bin_path: str | None = None,
    overwrite: bool = False,
) -> None:
    """ """
    logger.info("Loading overture buildings")
    tools.check_exists(overwrite, overture_schema_name, "overture_buildings")
    tools.prepare_schema(overture_schema_name)
    bounds_buffs = fetch_unioned_extents_4326(buffer_col, bounds_schema, bounds_table, bounds_fids)
    for bounds_row in tqdm(bounds_buffs):
        process_extent_buildings(
            bounds_row,
            overture_buildings_path,
            bounds_schema,
            bounds_table,
            overture_schema_name,
            buffer_col,
            bin_path,
        )


if __name__ == "__main__":
    """
    Examples are run from the project folder (the folder containing src)
    python -m src.data.load_overture_data 783 load_overture_networks eu bounds overture geom_10000 --overture_nodes_path='temp/eu_nodes.gpkg' --overture_edges_path='temp/eu_edges.gpkg' --overwrite=False
    python -m src.data.load_overture_data 783 load_overture_places eu bounds overture geom_2000 --overture_places_path='temp/eu_places.gpkg' --overwrite=False
    python -m src.data.load_overture_data 783 load_overture_buildings eu bounds overture geom_2000  --overture_buildings_path='temp/eu_buildings.gpkg' --overwrite=False
    """
    if True:
        parser = argparse.ArgumentParser(description="Load overture datasets to DB.")
        parser.add_argument(
            "bounds_fid",
            type=str,
            help="A bounds fid to load corresponding to input bounds table. Use 'all' to load all bounds.",
        )
        valid_loaders = ["load_overture_networks", "load_overture_places", "load_overture_buildings"]
        parser.add_argument(
            "loader",
            type=str,
            choices=valid_loaders,
            help="The loader to use; one of 'load_overture_networks', 'load_overture_places', 'load_overture_buildings'.",
        )
        parser.add_argument("bounds_schema", type=str, help="Schema name for input boundary polygons.")
        parser.add_argument("bounds_table", type=str, help="Table name for input boundary polygons.")
        parser.add_argument("overture_schema", type=str, help="Schema name for overture schema.")
        parser.add_argument("bounds_buffer_column", type=str, help="Column name for buffered boundary.")
        parser.add_argument(
            "--overture_nodes_path",
            type=str,
            help="Path to overture nodes dataset, required for 'load_overture_networks'.",
            default=None,
        )
        parser.add_argument(
            "--overture_edges_path",
            type=str,
            help="Path to overture edges dataset, required for 'load_overture_networks'.",
            default=None,
        )
        parser.add_argument(
            "--overture_places_path",
            type=str,
            help="Path to overture places dataset, required for 'load_overture_places'.",
            default=None,
        )
        parser.add_argument(
            "--overture_buildings_path",
            type=str,
            help="Path to overture buildings dataset, required for 'load_overture_buildings'.",
            default=None,
        )
        parser.add_argument("--bin_path", type=str, default=None, help="Optional bin path for ogr2ogr.")
        parser.add_argument("--overwrite", type=bool, default=False, help="Whether to overwrite existing tables.")
        args = parser.parse_args()
        bounds_fid = args.bounds_fid
        if bounds_fid == "all":
            bounds_fids = tools.db_fetch(
                f"""
                WITH fids AS (SELECT fid FROM {args.bounds_schema}.{args.bounds_table} ORDER BY fid ASC)
                SELECT array_agg(fid) FROM fids
                """
            )[0][0]
        else:
            bounds_fids = [int(bounds_fid)]
        if args.loader == "load_overture_networks":
            if args.overture_nodes_path is None or args.overture_edges_path is None:
                raise ValueError(
                    "overture_nodes_path and overture_edges_path arguments are required for load_overture_networks"
                )
            load_overture_networks(
                bounds_fids,
                args.overture_nodes_path,
                args.overture_edges_path,
                args.bounds_schema,
                args.bounds_table,
                args.overture_schema,
                args.bounds_buffer_column,
                args.bin_path,
                args.overwrite,
            )
        elif args.loader == "load_overture_places":
            if args.overture_places_path is None:
                raise ValueError("overture_places_path is required for load_overture_places")
            load_overture_places(
                bounds_fids,
                args.overture_places_path,
                args.bounds_schema,
                args.bounds_table,
                args.overture_schema,
                args.bounds_buffer_column,
                args.bin_path,
                args.overwrite,
            )
        elif args.loader == "load_overture_buildings":
            if args.overture_buildings_path is None:
                raise ValueError("overture_buildings_path is required for load_overture_buildings")
            load_overture_buildings(
                bounds_fids,
                args.overture_buildings_path,
                args.bounds_schema,
                args.bounds_table,
                args.overture_schema,
                args.bounds_buffer_column,
                args.bin_path,
                args.overwrite,
            )
    else:
        # bounds_fids = [783, 743, 751, 758, 761, 763, 764, 766, 767, 768, 769, 771]
        bounds_fids = tools.db_fetch(
            f"""
                WITH fids AS (SELECT fid FROM eu.bounds ORDER BY fid ASC)
                SELECT array_agg(fid) FROM fids
                """
        )[0][0]
        # load_overture_networks(
        #     bounds_fids,
        #     "temp/eu_nodes.gpkg",
        #     "temp/eu_edges.gpkg",
        #     "eu",
        #     "bounds",
        #     "overture",
        #     "geom_10000",
        #     overwrite=False,
        # )
        load_overture_places(
            bounds_fids, "temp/eu_places.gpkg", "eu", "bounds", "overture", "geom_2000", overwrite=False
        )
        # load_overture_buildings(
        #     bounds_fids, "temp/eu_buildings.gpkg", "eu", "bounds", "overture", "geom_2000", overwrite=False
        # )
