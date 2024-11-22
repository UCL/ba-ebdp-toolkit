""" """

from __future__ import annotations

import argparse

import geopandas as gpd
import numpy as np
from rasterio.enums import Resampling
from rasterio.io import MemoryFile
from rasterstats import point_query
from tqdm import tqdm

from src import tools

logger = tools.get_logger(__name__)


def process_population(
    bounds_fid: int,
    bounds_fid_col: str,
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
    if len(nodes_gdf) == 0:  # type: ignore
        raise OSError(f"No network data for bounds FID: {bounds_fid}")
    # track bounds
    nodes_gdf.loc[:, "bounds_key"] = "bounds"  # type: ignore
    nodes_gdf.loc[:, "bounds_fid"] = bounds_fid  # type: ignore
    # fetch population raster for boundary
    pop_raster = tools.db_fetch(
        f"""
        WITH bounds AS (
            SELECT geom
            FROM eu.{bounds_table}
            WHERE {bounds_fid_col} = {bounds_fid}
        ), rasters AS (
            SELECT rast 
            FROM eu.pop_dens, bounds as b
            WHERE ST_Intersects(rast, b.geom)
        ), mosaic AS (
            SELECT ST_Union(rasters.rast) as merged 
            FROM rasters
        )
        SELECT ST_AsTiff(ST_Clip(m.merged, b.geom))
            FROM mosaic m, bounds as b
            LIMIT 1;
        """
    )[0][0]
    upscale_factor = 10
    if pop_raster is None:
        nodes_gdf["pop_dens"] = np.nan  # type: ignore
    else:
        with MemoryFile(pop_raster) as memfile:
            with memfile.open() as dataset:
                # rea
                data = dataset.read(
                    out_shape=(
                        dataset.count,
                        int(dataset.height * upscale_factor),
                        int(dataset.width * upscale_factor),
                    ),
                    resampling=Resampling.bilinear,
                )
                # Update the transform to reflect the new shape
                old_trf = dataset.transform
                new_trf = old_trf * old_trf.scale((dataset.width / data.shape[-1]), (dataset.height / data.shape[-2]))
                for node_idx, node_row in tqdm(nodes_gdf.iterrows(), total=len(nodes_gdf)):  # type: ignore
                    pop_val = point_query(
                        node_row["geom"],
                        data,
                        interpolate="nearest",
                        affine=new_trf,
                        nodata=np.nan,
                    )
                    nodes_gdf.loc[node_idx, "pop_dens"] = np.clip(pop_val, 0, np.inf)[0]  # type: ignore
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


def compute_population_metrics(
    target_bounds_fids: list[int] | str,
    drop: bool = False,
):
    if not (
        tools.check_table_exists("overture", "network_nodes_clean")
        and tools.check_table_exists("overture", "network_edges_clean")
    ):
        raise OSError("The cleaned network nodes and edges tables need to be created prior to proceeding.")
    logger.info("Computing population metrics")
    tools.prepare_schema("metrics")
    load_key = "metrics_population"
    bounds_schema = "eu"
    # use eu bounds not unioned_bounds - use geom_2000 for geom column
    bounds_table = "bounds"
    bounds_geom_col = "geom_2000"
    bounds_fid_col = "fid"
    target_schema = "metrics"
    target_table = "population"
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
            core_function=process_population,
            func_args=[
                bound_fid,
                bounds_fid_col,
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
    python -m src.processing.metrics_population all
    """

    if True:
        parser = argparse.ArgumentParser(description="Compute population metrics.")
        parser.add_argument(
            "bounds_fid",
            type=tools.bounds_fid_type,
            help=("A bounds fid as int to load a specific bounds. Use 'all' to load all bounds."),
        )
        parser.add_argument("--drop", action="store_true", help="Whether to drop existing tables.")
        args = parser.parse_args()
        compute_population_metrics(
            args.bounds_fid,
            drop=args.drop,
        )
    else:
        bounds_fids = [517]
        compute_population_metrics(
            bounds_fids,
            drop=False,
        )
