""" """

import argparse
import os
import shutil
import zipfile
from pathlib import Path

import fiona
import geopandas as gpd
from shapely import geometry, wkb
from tqdm import tqdm

from src import tools

logger = tools.get_logger(__name__)


def load_tree_canopies(data_dir_path: str) -> None:
    """ """
    # check that the bounds table exists
    if not tools.check_table_exists("eu", "bounds"):
        raise OSError("The eu.bounds table does not exist; this needs to be created prior to proceeding.")
    # drop existing
    tools.drop_table("eu", "trees")
    # get bounds poly
    bounds_wkb: str = tools.db_fetch(
        """
        SELECT ST_Union(geom_2000) FROM eu.bounds;
        """
    )[0][0]
    bounds_geom: geometry.Polygon = wkb.loads(bounds_wkb, hex=True)  # type: ignore
    if not (isinstance(bounds_geom, geometry.Polygon | geometry.MultiPolygon)):
        raise ValueError(
            f"Encountered {bounds_geom.type} instead of Polygon or MultiPolygon type for bounds."  # type: ignore
        )
    # prepare engine for GDF
    engine = tools.get_sqlalchemy_engine()
    # iter zip files and load if intersecting bounds
    dir_path: Path = Path(data_dir_path)
    unzip_dir = dir_path / "temp_unzipped/"
    for zip_file_name in tqdm(os.listdir(dir_path)):
        if zip_file_name.endswith(".zip"):
            # Create directory for unzipped files
            if os.path.exists(unzip_dir):
                shutil.rmtree(unzip_dir)
            os.makedirs(unzip_dir)
            full_zip_path = dir_path / zip_file_name
            # Unzip
            with zipfile.ZipFile(full_zip_path, "r") as zip_ref:
                zip_ref.extractall(unzip_dir)
            # Extract features from the geopackage file
            for walk_dir_path, _dir_names, file_names in os.walk(unzip_dir):
                for file_name in file_names:
                    if file_name.endswith(".gpkg"):
                        full_gpkg_path = str((Path(walk_dir_path) / file_name).resolve())
                        # use fiona for quick bbox check
                        with fiona.open(full_gpkg_path) as src:  # type: ignore
                            if not geometry.box(*src.bounds).intersects(bounds_geom):  # type: ignore
                                continue
                        gdf = gpd.read_file(full_gpkg_path)  # type: ignore
                        # filter spatially
                        gdf["bbox"] = gdf["geometry"].envelope
                        gdf.set_geometry("bbox", inplace=True)
                        gdf_itx = gdf[gdf.intersects(bounds_geom)]
                        gdf_itx.rename(columns={"geometry": "geom"}, inplace=True)
                        gdf_itx.set_geometry("geom", inplace=True)
                        # explode multipolygons
                        gdf_exp = gdf_itx.explode(index_parts=False)
                        # write to postgis
                        cols = ["fua_name", "fua_code", "geom"]
                        gdf_exp[cols].to_postgis(
                            "trees",
                            engine,
                            if_exists="append",
                            schema="eu",
                            index=True,
                            index_label="temp_fid",
                        )
            # Delete the unzipped files
            shutil.rmtree(unzip_dir)
    tools.db_execute(
        """
        ALTER TABLE eu.trees ADD COLUMN fid serial;
        ALTER TABLE eu.trees ADD PRIMARY KEY (fid);
        ALTER TABLE eu.trees DROP COLUMN temp_fid;
        """
    )


if __name__ == "__main__":
    """
    Examples are run from the project folder (the folder containing src)
    python -m src.data.load_urban_atlas_trees "./temp/urban"
    """
    if True:
        parser = argparse.ArgumentParser(description="Load building heights raster data.")
        parser.add_argument("data_dir_path", type=str, help="Input data directory with zipped data files.")
        args = parser.parse_args()
        logger.info(f"Loading trees data from path: {args.data_dir_path}")
        data_dir_path = Path(args.data_dir_path)
        if not data_dir_path.exists():
            raise OSError("Input directory does not exist")
        if not data_dir_path.is_dir():
            raise OSError("Expected input directory, not a file name")
        load_tree_canopies(args.data_dir_path)
    else:
        load_tree_canopies("./temp/urban atlas trees")
