"""
Airports
Arable land (annual crops)
Complex and mixed cultivation patterns
Construction sites
Continuous urban fabric (S.L. : > 80%)
Discontinuous dense urban fabric (S.L. : 50% -  80%)
Discontinuous low density urban fabric (S.L. : 10% - 30%)
Discontinuous medium density urban fabric (S.L. : 30% - 50%)
Discontinuous very low density urban fabric (S.L. : < 10%)
Forests
Green urban areas
Herbaceous vegetation associations (natural grassland, moors...)
Industrial, commercial, public, military and private units
Isolated structures
Land without current use
Mineral extraction and dump sites
Open spaces with little or no vegetation (beaches, dunes, bare rocks, glaciers)
Orchards at the fringe of urban classes
Pastures
Permanent crops (vineyards, fruit trees, olive groves)
Port areas
Sports and leisure facilities
Water
Wetlands
"""

import argparse

import geopandas as gpd
import numpy as np
import rasterio
import scipy.ndimage
from rasterio.features import rasterize
from rasterio.io import MemoryFile
from rasterio.transform import from_origin
from shapely import geometry
from tqdm import tqdm

from src import tools

logger = tools.get_logger(__name__)


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
            c.x,
            c.y,
            ST_Contains(b.geom, ST_Centroid(c.primal_edge)) as live,
            c.primal_edge as geom
        FROM overture.dual_nodes c, eu.{bounds_table} b
        WHERE b.{bounds_fid_col} = {bounds_fid}
                AND ST_Intersects(b.geom, c.primal_edge)
                AND ST_Contains(b.geom, ST_Centroid(c.primal_edge));
        """,
        engine,
        index_col="fid",
        geom_col="geom",
    )
    if len(nodes_gdf) == 0:  # type: ignore
        raise OSError(f"No network data for bounds FID: {bounds_fid}")
    # track bounds
    nodes_gdf.loc[:, "bounds_key"] = "bounds"  # type: ignore
    nodes_gdf.loc[:, "bounds_fid"] = bounds_fid  # type: ignore
    # green spaces
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
    # trees
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
    logger.info("Preparing transform")
    # buffer bounds
    bounds = geometry.box(*nodes_gdf.total_bounds).buffer(2000).bounds  # type: ignore
    pixel_size = 10
    num_cols = int((bounds[2] - bounds[0]) / pixel_size)
    num_rows = int((bounds[3] - bounds[1]) / pixel_size)
    transform = from_origin(bounds[0], bounds[3], pixel_size, pixel_size)
    # create trees raster
    for dist in [100, 500]:
        # prepare kernel
        cell_dist = np.ceil(dist / pixel_size).astype(int)
        kernel_size = 2 * cell_dist + 1
        kernel = np.zeros((kernel_size, kernel_size), dtype=np.uint8)
        y, x = np.ogrid[-cell_dist : cell_dist + 1, -cell_dist : cell_dist + 1]
        mask = x**2 + y**2 <= cell_dist**2
        kernel[mask] = 1
        # iter GDFs
        for data_key, data_gdf in [("trees", trees_gdf), ("green", green_gdf)]:
            logger.info(f"Processing {data_key} at distance {dist}")
            if len(data_gdf) == 0:
                nodes_gdf[f"{data_key}_{dist}"] = 0  # type ignore
                logger.warning(f"Missing data for {data_key} in bounds FID {bounds_fid}")
                continue
            logger.info("Burning shapes")
            with (
                MemoryFile() as memfile,
                memfile.open(
                    driver="GTiff",
                    height=num_rows,
                    width=num_cols,
                    count=1,
                    dtype=rasterio.uint8,
                    crs=nodes_gdf.crs,  # type: ignore
                    transform=transform,
                ) as burn_rast,
            ):
                shapes = ((geom, 1) for geom in data_gdf.geometry)
                burned = rasterize(shapes=shapes, out_shape=(num_rows, num_cols), transform=transform)
                burn_rast.write_band(1, burned)
                logger.info("Convolving distances")
                with (
                    MemoryFile() as memfile2,
                    memfile2.open(
                        driver="GTiff",
                        height=num_rows,
                        width=num_cols,
                        count=1,
                        dtype=rasterio.uint32,
                        crs=nodes_gdf.crs,  # type: ignore
                        transform=transform,
                    ) as conv_rast,
                ):
                    count_ones = scipy.ndimage.convolve(burned, kernel, mode="constant", cval=0, output=np.uint32)
                    conv_rast.write_band(1, count_ones)
                    logger.info("Sampling")
                    for idx, row in nodes_gdf.iterrows():  # type: ignore
                        for val in conv_rast.sample([(row.geom.centroid.x, row.geom.centroid.y)]):
                            # reset area to pixel size then take km2
                            nodes_gdf.at[idx, f"{data_key}_{dist}"] = (val[0] * pixel_size**2) / 1000**2  # type: ignore
    # keep only live
    nodes_gdf = nodes_gdf.loc[nodes_gdf.live]  # type: ignore
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
    if not (tools.check_table_exists("overture", "dual_nodes") and tools.check_table_exists("overture", "dual_edges")):
        raise OSError("The cleaned network nodes and edges tables need to be created prior to proceeding.")
    logger.info("Computing green metrics")
    tools.prepare_schema("metrics")
    load_key = "metrics_green_conv"
    bounds_schema = "eu"
    # use eu bounds not unioned_bounds - use geom_2000 for geom column
    bounds_table = "bounds"
    bounds_geom_col = "geom_2000"
    bounds_fid_col = "fid"
    target_schema = "metrics"
    target_table = "green_conv"
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

    if True:
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
        bounds_fids = [636]
        compute_green_metrics(
            bounds_fids,
            drop=False,
        )
