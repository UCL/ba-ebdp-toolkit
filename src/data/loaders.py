""" """

import logging

import geopandas as gpd
from cityseer.tools import io
from overturemaps import core
from shapely import geometry

from src import tools

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def load_network(
    bounds_geom_wgs: geometry.Polygon,
    crs: int,
) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame, gpd.GeoDataFrame]:
    """ """
    # NODES
    logger.info("Loading nodes")
    nodes_gdf = core.geodataframe("connector", bounds_geom_wgs.bounds)  # type:ignore
    nodes_gdf.set_crs(4326, inplace=True)
    nodes_gdf.to_crs(crs, inplace=True)
    nodes_gdf.set_index("id", inplace=True)
    nodes_gdf.rename(columns={"geometry": "geom"}, inplace=True)
    nodes_gdf.set_geometry("geom", inplace=True)
    nodes_gdf.drop(columns=["bbox"], inplace=True)
    # EDGES
    logger.info("Loading edges")
    edges_gdf = core.geodataframe("segment", bounds_geom_wgs.bounds)  # type:ignore
    edges_gdf.set_crs(4326, inplace=True)
    edges_gdf.to_crs(crs, inplace=True)
    edges_gdf.set_index("id", inplace=True)
    edges_gdf.rename(columns={"geometry": "geom"}, inplace=True)
    edges_gdf.set_geometry("geom", inplace=True)
    edges_gdf.drop(columns=["bbox"], inplace=True)
    # CLEAN
    logger.info("Cleaning network")
    edges_gdf = edges_gdf[edges_gdf["subtype"] == "road"]  # type: ignore
    multigraph = tools.generate_graph(
        nodes_gdf=nodes_gdf,  # type: ignore
        edges_gdf=edges_gdf,  # type: ignore
        # not dropping "parking_aisle" because this sometimes removes important links
    )
    multigraph = io._auto_clean_network(
        multigraph,
        geom_wgs=bounds_geom_wgs,
        to_crs_code=crs,
        final_clean_distances=(5, 10),
        remove_disconnected=100,
        green_footways=True,
        green_service_roads=False,
    )
    clean_edges_gdf = io.geopandas_from_nx(multigraph, crs=crs)
    # JSON
    nodes_gdf["sources"] = nodes_gdf["sources"].apply(tools.col_to_json)  # type: ignore
    for col in [
        "sources",
        "names",
        "connectors",
        "routes",
        "subclass_rules",
        "access_restrictions",
        "level_rules",
        "destinations",
        "prohibited_transitions",
        "road_surface",
        "road_flags",
        "speed_limits",
        "width_rules",
    ]:
        edges_gdf[col] = edges_gdf[col].apply(tools.col_to_json).astype("str")  # type: ignore
    # trim
    logger.info("Trimming to bounds")
    bounds_geom_crs = tools.reproject_geometry(bounds_geom_wgs, 4326, crs)
    nodes_gdf = nodes_gdf[nodes_gdf.intersects(bounds_geom_crs)]
    edges_gdf = edges_gdf[edges_gdf.intersects(bounds_geom_crs)]
    clean_edges_gdf = clean_edges_gdf[clean_edges_gdf.intersects(bounds_geom_crs)]

    return nodes_gdf, edges_gdf, clean_edges_gdf  # type: ignore


def load_buildings(
    bounds_geom_wgs: geometry.Polygon,
    crs: int,
) -> gpd.GeoDataFrame:
    """ """
    logger.info("Loading buildings")
    buildings_gdf = core.geodataframe("building", bounds_geom_wgs.bounds)  # type:ignore
    buildings_gdf.set_crs(4326, inplace=True)
    buildings_gdf.to_crs(crs, inplace=True)
    buildings_gdf.set_index("id", inplace=True)
    buildings_gdf.rename(columns={"geometry": "geom"}, inplace=True)
    buildings_gdf.set_geometry("geom", inplace=True)
    buildings_gdf.drop(columns=["bbox"], inplace=True)
    for col in ["sources", "names"]:
        buildings_gdf[col] = buildings_gdf[col].apply(tools.col_to_json).astype("str")  # type: ignore
    bounds_geom_crs = tools.reproject_geometry(bounds_geom_wgs, 4326, crs)
    buildings_gdf = buildings_gdf[buildings_gdf.intersects(bounds_geom_crs)]

    return buildings_gdf  # type: ignore


def load_infrastructure(
    bounds_geom_wgs: geometry.Polygon,
    crs: int,
) -> gpd.GeoDataFrame:
    """ """
    # INFRASTRUCTURE
    logger.info("Loading infrastructure")
    infrast_gdf = core.geodataframe("infrastructure", bounds_geom_wgs.bounds)  # type:ignore
    infrast_gdf.set_crs(4326, inplace=True)
    infrast_gdf.to_crs(crs, inplace=True)
    infrast_gdf.set_index("id", inplace=True)
    infrast_gdf.rename(columns={"geometry": "geom"}, inplace=True)
    infrast_gdf.set_geometry("geom", inplace=True)
    infrast_gdf = infrast_gdf[infrast_gdf.geom.geom_type == "Point"]  # returns line and polygons as well
    infrast_gdf.drop(columns=["bbox"], inplace=True)

    def extract_infrast_name(names: dict | None) -> str | None:
        if names is None:
            return None
        if names["common"] is not None:
            return names["common"]
        if names["primary"] is not None:
            return names["primary"]
        return None

    infrast_gdf["common_name"] = infrast_gdf["names"].apply(extract_infrast_name)  # type: ignore
    for col in [
        "sources",
        "names",
        "source_tags",
    ]:
        infrast_gdf[col] = infrast_gdf[col].apply(tools.col_to_json).astype(str)  # type: ignore
    bounds_geom_crs = tools.reproject_geometry(bounds_geom_wgs, 4326, crs)
    infrast_gdf = infrast_gdf[infrast_gdf.intersects(bounds_geom_crs)]

    return infrast_gdf  # type: ignore


def load_places(
    bounds_geom_wgs: geometry.Polygon,
    crs: int,
) -> gpd.GeoDataFrame:
    """ """
    OVERTURE_SCHEMA = tools.generate_overture_schema()
    # PLACES
    logger.info("Loading places")
    places_gdf = core.geodataframe("place", bounds_geom_wgs.bounds)  # type:ignore
    places_gdf.set_crs(4326, inplace=True)
    places_gdf.to_crs(crs, inplace=True)
    places_gdf.set_index("id", inplace=True)
    places_gdf.rename(columns={"geometry": "geom"}, inplace=True)
    places_gdf.set_geometry("geom", inplace=True)
    places_gdf.drop(columns=["bbox"], inplace=True)

    def extract_main_cat(lu_classes: dict | None) -> str | None:
        if lu_classes is None:
            return None
        return lu_classes["primary"]

    def extract_alt_cats(lu_classes: dict | None):
        if lu_classes is None:
            return None
        return tools.col_to_json(lu_classes["alternate"])

    def extract_name(names: dict | None) -> str | None:
        if names is None:
            return None
        if names["common"] is not None:
            return names["common"]
        if names["primary"] is not None:
            return names["primary"]
        return None

    def assign_major_cat(lu_cat_desc: str) -> str | None:
        for major_cat, major_cat_vals in OVERTURE_SCHEMA.items():
            if lu_cat_desc in major_cat_vals:
                return major_cat
        if lu_cat_desc is not None:
            logger.info(f"Category not found in landuse schema: {lu_cat_desc}")
        return None

    places_gdf["main_cat"] = places_gdf["categories"].apply(extract_main_cat)  # type: ignore
    places_gdf["alt_cats"] = places_gdf["categories"].apply(extract_alt_cats)  # type: ignore
    places_gdf["common_name"] = places_gdf["names"].apply(extract_name)  # type: ignore
    places_gdf["major_lu_schema_class"] = places_gdf["main_cat"].apply(assign_major_cat)  # type: ignore
    for col in [
        "sources",
        "names",
        "categories",
        "brand",
        "addresses",
        "websites",
        "socials",
        "emails",
        "phones",
    ]:
        places_gdf[col] = places_gdf[col].apply(tools.col_to_json).astype(str)  # type: ignore
    bounds_geom_crs = tools.reproject_geometry(bounds_geom_wgs, 4326, crs)
    places_gdf = places_gdf[places_gdf.intersects(bounds_geom_crs)]

    return places_gdf  # type: ignore
