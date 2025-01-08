""" """

import geopandas as gpd
import momepy
import numpy as np
from cityseer.metrics import layers, networks
from rasterio.io import MemoryFile
from rasterio.mask import mask

from src import tools

logger = tools.get_logger(__name__)

OVERTURE_SCHEMA = tools.generate_overture_schema()


def process_centrality(nodes_gdf: gpd.GeoDataFrame, network_structure) -> gpd.GeoDataFrame:
    """ """
    logger.info("Computing centrality")
    nodes_gdf = networks.node_centrality_shortest(
        network_structure, nodes_gdf, distances=[500, 1000, 2000, 5000, 10000]
    )
    return nodes_gdf


def process_places(
    nodes_gdf: gpd.GeoDataFrame, places_gdf: gpd.GeoDataFrame, infrast_gdf: gpd.GeoDataFrame, network_structure
) -> gpd.GeoDataFrame:
    """ """
    logger.info("Computing places")
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
    return nodes_gdf


def process_blocks_buildings(
    nodes_gdf: gpd.GeoDataFrame,
    bldgs_gdf: gpd.GeoDataFrame,
    blocks_gdf: gpd.GeoDataFrame,
    raster_bytes: bytes,
    network_structure,
) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame, gpd.GeoDataFrame]:
    """ """
    logger.info("Computing morphology")
    # placeholders
    for col_key in [
        "area",
        "perimeter",
        "compactness",
        "orientation",
        "volume",
        "floor_area_ratio",
        "form_factor",
        "corners",
        "shape_index",
        "fractal_dimension",
    ]:
        bldgs_gdf.loc[:, col_key] = np.nan
    if not bldgs_gdf.empty:
        # explode
        bldgs_gdf = bldgs_gdf.explode(index_parts=False)  # type: ignore
        bldgs_gdf.reset_index(drop=True, inplace=True)
        bldgs_gdf.index = bldgs_gdf.index.astype(str)
        # sample heights
        heights = []
        with MemoryFile(raster_bytes) as memfile:
            rast_data = memfile.open()
            for _idx, bldg_row in bldgs_gdf.iterrows():
                try:
                    # raster values within building polygon
                    out_image, _ = mask(rast_data, [bldg_row.geom.buffer(5)], crop=True)
                    # mean height, excluding nodata values
                    valid_pixels = out_image[0][out_image[0] != rast_data.nodata]
                    mean_height = np.mean(valid_pixels) if len(valid_pixels) > 0 else np.nan
                    heights.append(mean_height)
                except ValueError:
                    heights.append(np.nan)
        bldgs_gdf["mean_height"] = heights
        # bldg metrics
        area = bldgs_gdf.area
        ht = bldgs_gdf.loc[:, "mean_height"]
        bldgs_gdf["area"] = area
        bldgs_gdf["perimeter"] = bldgs_gdf.length
        bldgs_gdf["compactness"] = momepy.circular_compactness(bldgs_gdf)
        bldgs_gdf["orientation"] = momepy.orientation(bldgs_gdf)
        # height-based metrics
        bldgs_gdf["volume"] = momepy.volume(area, ht)
        bldgs_gdf["floor_area_ratio"] = momepy.floor_area(area, ht, 3)
        bldgs_gdf["form_factor"] = momepy.form_factor(bldgs_gdf, ht)
        # complexity metrics
        bldgs_gdf["corners"] = momepy.corners(bldgs_gdf)
        bldgs_gdf["shape_index"] = momepy.shape_index(bldgs_gdf)
        bldgs_gdf["fractal_dimension"] = momepy.fractal_dimension(bldgs_gdf)
    # calculate
    bldgs_gdf["centroid"] = bldgs_gdf.geometry.centroid
    bldgs_gdf.set_geometry("centroid", inplace=True)
    for col_key in [
        "area",
        "perimeter",
        "compactness",
        "orientation",
        "volume",
        "floor_area_ratio",
        "form_factor",
        "corners",
        "shape_index",
        "fractal_dimension",
    ]:
        nodes_gdf, bldgs_gdf = layers.compute_stats(
            data_gdf=bldgs_gdf,
            stats_column_label=col_key,
            nodes_gdf=nodes_gdf,
            network_structure=network_structure,
            distances=[100, 500, 1500],
        )
        trim_columns = []
        for column_name in nodes_gdf.columns:
            if col_key in column_name and not column_name.startswith(f"cc_{col_key}_mean"):
                trim_columns.append(column_name)
        nodes_gdf.drop(columns=trim_columns, inplace=True)
    # placeholders
    for col_key in [
        "block_area",
        "block_perimeter",
        "block_compactness",
        "block_orientation",
        "block_covered_ratio",
    ]:
        blocks_gdf.loc[:, col_key] = np.nan
    # block metrics
    if not blocks_gdf.empty:
        blocks_gdf.index = blocks_gdf.index.astype(str)
        blocks_gdf["block_area"] = blocks_gdf.area
        blocks_gdf["block_perimeter"] = blocks_gdf.length
        blocks_gdf["block_compactness"] = momepy.circular_compactness(blocks_gdf)
        blocks_gdf["block_orientation"] = momepy.orientation(blocks_gdf)
    # joint metrics require spatial join
    if not blocks_gdf.empty and not bldgs_gdf.empty:
        blocks_gdf["index_bl"] = blocks_gdf.index.values
        merged_gdf = gpd.sjoin(bldgs_gdf, blocks_gdf, how="left", predicate="intersects", lsuffix="bldg", rsuffix="bl")
        blocks_gdf["block_covered_ratio"] = momepy.AreaRatio(
            blocks_gdf, merged_gdf, "block_area", "area", left_unique_id="index_bl", right_unique_id="index_bl"
        ).series
    # calculate
    blocks_gdf["centroid"] = blocks_gdf.geometry.centroid
    blocks_gdf.set_geometry("centroid", inplace=True)
    for col_key in ["block_area", "block_perimeter", "block_compactness", "block_orientation", "block_covered_ratio"]:
        nodes_gdf, blocks_gdf = layers.compute_stats(
            data_gdf=blocks_gdf,
            stats_column_label=col_key,
            nodes_gdf=nodes_gdf,
            network_structure=network_structure,
            distances=[100, 500, 1500],
        )
        trim_columns = []
        for column_name in nodes_gdf.columns:
            if col_key in column_name and not column_name.startswith(f"cc_{col_key}_mean"):
                trim_columns.append(column_name)
        nodes_gdf.drop(columns=trim_columns, inplace=True)
    # reset geometry
    bldgs_gdf.set_geometry("geom", inplace=True)
    blocks_gdf.set_geometry("geom", inplace=True)
    #
    return nodes_gdf, bldgs_gdf, blocks_gdf


def process_green(
    nodes_gdf: gpd.GeoDataFrame, green_gdf: gpd.GeoDataFrame, trees_gdf: gpd.GeoDataFrame, network_structure
) -> gpd.GeoDataFrame:
    """ """
    logger.info("Computing green")

    # function for extracting points
    def generate_points(fid, categ, polygon, interval=20, simplify=20):
        if polygon.is_empty or polygon.exterior.length == 0:
            return []
        ring = polygon.exterior.simplify(simplify)
        num_points = int(ring.length // interval)
        return [(fid, categ, ring.interpolate(distance)) for distance in range(0, num_points * interval, interval)]

    # extract points
    points = []
    # for green
    for fid, geom in zip(green_gdf.index, green_gdf.geom, strict=True):  # type: ignore
        if geom.geom_type == "Polygon":
            points.extend(generate_points(fid, "green", geom, interval=20, simplify=10))
    # for trees
    for fid, geom in zip(trees_gdf.index, trees_gdf.geom, strict=True):  # type: ignore
        if geom.geom_type == "Polygon":
            points.extend(generate_points(fid, "trees", geom, interval=20, simplify=5))
    # create GDF
    points_gdf = gpd.GeoDataFrame(  # type: ignore
        points,
        columns=["fid", "cat", "geometry"],
        geometry="geometry",
        crs=trees_gdf.crs,  # type: ignore
    )
    points_gdf.index = points_gdf.index.astype(str)
    # compute accessibilities
    nodes_gdf, points_gdf = layers.compute_accessibilities(
        points_gdf,  # type: ignore
        landuse_column_label="cat",
        accessibility_keys=["green", "trees"],
        nodes_gdf=nodes_gdf,
        network_structure=network_structure,
        distances=[1500],
        data_id_col="fid",  # deduplicate
    )
    # drop - aggregation columns since these are not meaningful for interpolated aggs - only using distances
    nodes_gdf = nodes_gdf.drop(
        columns=[
            "cc_green_1500_nw",
            "cc_green_1500_wt",
            "cc_trees_1500_nw",
            "cc_trees_1500_wt",
        ]
    )
    # set contained green nodes to zero
    contained_green_idx = gpd.sjoin(nodes_gdf, green_gdf, predicate="intersects", how="inner")
    nodes_gdf.loc[contained_green_idx.index, "cc_green_nearest_max_1500"] = 0
    # same for trees
    contained_trees_idx = gpd.sjoin(nodes_gdf, trees_gdf, predicate="intersects", how="inner")
    nodes_gdf.loc[contained_trees_idx.index, "cc_trees_nearest_max_1500"] = 0

    return nodes_gdf
