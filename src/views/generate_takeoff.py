""" """

from pathlib import Path

import geopandas as gpd

from src import tools

OVERTURE_SCHEMA = tools.generate_overture_schema()

engine = tools.get_sqlalchemy_engine()
logger = tools.get_logger(__name__)


def prepare_metrics_takeoffs(city_key: str, bounds_fid: int):
    """ """
    logger.info(f"Processing {city_key}")
    tools.db_execute("CREATE SCHEMA IF NOT EXISTS takeoffs")
    logger.info("Checking for index")
    for metrics_table in ["segment_metrics", "blocks", "buildings"]:
        tools.db_execute(
            f"CREATE INDEX IF NOT EXISTS idx_{metrics_table}_bounds_fid ON metrics.{metrics_table} (bounds_fid);"
        )
        logger.info("Reading")
        bounds_data = gpd.read_postgis(
            f"""
            SELECT *
            FROM metrics.{metrics_table}
            WHERE bounds_fid = {bounds_fid};
            """,
            engine,
            index_col="fid",
            geom_col="geom",
        )
        logger.info("Writing")
        bounds_data.to_postgis(  # type: ignore
            f"b_{city_key}_{metrics_table}",
            engine,
            if_exists="replace",
            schema="takeoffs",
            index=True,
            index_label="fid",
        )


def prepare_metrics_parquet(bounds_fids: list[int], out_path: str):
    """ """
    bounds_fids = list(sorted(bounds_fids))
    fids = [str(i) for i in bounds_fids]
    metrics_table = "segment_metrics"
    tools.db_execute(
        f"CREATE INDEX IF NOT EXISTS idx_{metrics_table}_bounds_fid ON metrics.{metrics_table} (bounds_fid);"
    )
    cols = [
        "fid",
        "x",
        "y",
        "m.geom",
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
        "bounds_key",
        "bounds_fid",
        "cc_beta_500 ",
        "cc_beta_1000",
        "cc_beta_2000",
        "cc_beta_5000",
        "cc_beta_10000",
        "cc_harmonic_500",
        "cc_harmonic_1000",
        "cc_harmonic_2000",
        "cc_harmonic_5000",
        "cc_harmonic_10000",
        "cc_betweenness_500",
        "cc_betweenness_1000",
        "cc_betweenness_2000",
        "cc_betweenness_5000",
        "cc_betweenness_10000",
        "cc_betweenness_beta_500",
        "cc_betweenness_beta_1000",
        "cc_betweenness_beta_2000",
        "cc_betweenness_beta_5000",
        "cc_betweenness_beta_10000",
        "cc_hill_q0_100_nw",
        "cc_hill_q0_500_nw",
        "cc_hill_q0_1500_nw",
        "cc_hill_q0_1500_wt",
        "cc_area_mean_100_nw",
        "cc_area_mean_500_nw",
        "cc_area_mean_1500_nw",
        "cc_area_mean_1500_wt",
        "cc_perimeter_mean_100_nw",
        "cc_perimeter_mean_500_nw",
        "cc_perimeter_mean_1500_nw",
        "cc_perimeter_mean_1500_wt",
        "cc_compactness_mean_100_nw",
        "cc_compactness_mean_500_nw",
        "cc_compactness_mean_1500_nw",
        "cc_compactness_mean_1500_wt",
        "cc_orientation_mean_100_nw",
        "cc_orientation_mean_500_nw",
        "cc_orientation_mean_1500_nw",
        "cc_orientation_mean_1500_wt",
        "cc_volume_mean_100_nw",
        "cc_volume_mean_500_nw",
        "cc_volume_mean_1500_nw",
        "cc_volume_mean_1500_wt",
        "cc_floor_area_ratio_mean_100_nw",
        "cc_floor_area_ratio_mean_500_nw",
        "cc_floor_area_ratio_mean_1500_nw",
        "cc_floor_area_ratio_mean_1500_wt",
        "cc_form_factor_mean_100_nw",
        "cc_form_factor_mean_500_nw",
        "cc_form_factor_mean_1500_nw",
        "cc_form_factor_mean_1500_wt",
        "cc_corners_mean_100_nw",
        "cc_corners_mean_500_nw",
        "cc_corners_mean_1500_nw",
        "cc_corners_mean_1500_wt",
        "cc_shape_index_mean_100_nw",
        "cc_shape_index_mean_500_nw",
        "cc_shape_index_mean_1500_nw",
        "cc_shape_index_mean_1500_wt",
        "cc_fractal_dimension_mean_100_nw",
        "cc_fractal_dimension_mean_500_nw",
        "cc_fractal_dimension_mean_1500_nw",
        "cc_fractal_dimension_mean_1500_wt",
        "cc_block_area_mean_100_nw",
        "cc_block_area_mean_500_nw",
        "cc_block_area_mean_1500_nw",
        "cc_block_area_mean_1500_wt",
        "cc_block_perimeter_mean_100_nw",
        "cc_block_perimeter_mean_500_nw",
        "cc_block_perimeter_mean_1500_nw",
        "cc_block_perimeter_mean_1500_wt",
        "cc_block_compactness_mean_100_nw",
        "cc_block_compactness_mean_500_nw",
        "cc_block_compactness_mean_1500_nw",
        "cc_block_compactness_mean_1500_wt",
        "cc_block_orientation_mean_100_nw",
        "cc_block_orientation_mean_500_nw",
        "cc_block_orientation_mean_1500_nw",
        "cc_block_orientation_mean_1500_wt",
        "cc_block_covered_ratio_mean_100_nw",
        "cc_block_covered_ratio_mean_500_nw",
        "cc_block_covered_ratio_mean_1500_nw",
        "cc_block_covered_ratio_mean_1500_wt",
        "cc_green_nearest_max_1500",
        "cc_trees_nearest_max_1500",
    ]
    for c in OVERTURE_SCHEMA:
        if c in ["structure_and_geography", "mass_media"]:
            continue
        cols.extend(
            [
                f"cc_{c}_100_nw",
                f"cc_{c}_500_nw",
                f"cc_{c}_1500_nw",
                f"cc_{c}_1500_wt",
                f"cc_{c}_nearest_max_1500",
            ]
        )
    logger.info("Reading")
    bounds_gdf = gpd.read_postgis(
        f"""
        WITH bounds AS (
            SELECT geom
            FROM eu.bounds
            WHERE fid in ({", ".join(fids)})
        )
        SELECT {", ".join(cols)}
        FROM metrics.{metrics_table} m
        JOIN bounds b ON ST_Intersects(b.geom, m.geom);
        """,
        engine,
        index_col="fid",
        geom_col="geom",
    )
    logger.info("Writing")
    full_path = Path(out_path) / f"eu_segment_metrics_{fids[0]}_{fids[-1]}.parquet"
    bounds_gdf.to_parquet(str(full_path), index=True)  # type: ignore


def prepare_data_takeoffs(city_key: str, bounds_fid_2km: int, bounds_fid_10km: int):
    """ """
    logger.info(f"Processing {city_key}")
    tools.db_execute("CREATE SCHEMA IF NOT EXISTS takeoffs")
    for schema, table, bounds_table, bounds_fid in [
        ("overture", "dual_edges", "unioned_bounds_10000", bounds_fid_10km),  # uses 10km
        ("overture", "overture_buildings", "unioned_bounds_2000", bounds_fid_2km),  # uses 2km
        ("overture", "overture_infrast", "unioned_bounds_2000", bounds_fid_2km),
        ("overture", "overture_place", "unioned_bounds_2000", bounds_fid_2km),
        ("eu", "blocks", "unioned_bounds_2000", bounds_fid_2km),  # uses 2km
        ("eu", "stats", "unioned_bounds_2000", bounds_fid_2km),  # uses 2km
        ("eu", "trees", "unioned_bounds_2000", bounds_fid_2km),  # uses 2km
        ("eu", "bounds", "unioned_bounds_2000", bounds_fid_2km),  # uses 2km
    ]:
        logger.info(f"Processing {table}")
        logger.info("Reading")
        data = gpd.read_postgis(
            f"""
            WITH bounds AS (
                SELECT geom
                FROM eu.{bounds_table}
                WHERE fid = {bounds_fid}
            )
            SELECT ot.*
            FROM {schema}.{table} ot
            JOIN bounds b ON ST_Intersects(b.geom, ot.geom);
            """,
            engine,
            index_col="fid",
            geom_col="geom",
        )
        logger.info("Writing")
        data.to_postgis(  # type:ignore
            f"{city_key}_{table}",
            engine,
            if_exists="replace",
            schema="takeoffs",
            index=True,
            index_label="fid",
        )
        # GPKG expects int index
        data.reset_index(drop=True, inplace=True)  # type:ignore
        data.to_file(f"temp/{city_key}_{table}.gpkg")  # type:ignore


if __name__ == "__main__":
    """ """
    # prepare_data_takeoffs("nicosia", 105, 119)
    # prepare_data_takeoffs("madrid", 6, 7)
    # prepare_metrics_takeoffs("berlin", 100)
    prepare_metrics_parquet(list(range(1, 201)), "temp")
