from __future__ import annotations

import argparse
import json
import os
import shutil
import zipfile
from pathlib import Path

import fiona
import geopandas as gpd
from dotenv import load_dotenv
from shapely import geometry, wkb
from sqlalchemy import create_engine, text
from tqdm import tqdm

from src.tools import get_logger

logger = get_logger(__name__)

load_dotenv()

db_config_json = os.getenv("DB_CONFIG")
db_config = json.loads(db_config_json)
connection_string = (
    f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}@{db_config['host']}/{db_config['database']}"
)
engine = create_engine(connection_string)


def load_urban_blocks(dir_path_str: str, schema_name: str, bounds_table_name: str, atlas_table_name: str) -> None:
    """ """
    # don't overwrite existing
    with engine.connect() as connection:
        table_exists: bool = connection.execute(  # type: ignore
            text(
                f"""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables
                        WHERE table_schema = '{schema_name}' 
                            AND table_name = '{atlas_table_name}');
            """
            )
        ).fetchone()[0]
    if table_exists:
        raise IOError(f"Destination schema and table {schema_name}.{atlas_table_name} already exists; aborting.")
    # get bounds poly
    with engine.connect() as connection:
        bounds_wkb: str = connection.execute(  # type: ignore
            text(
                f"""
                SELECT ST_Union(ST_Buffer(geom, 2000))
                    FROM {schema_name}.{bounds_table_name};
            """
            )
        ).fetchone()[0]
    bounds_geom = wkb.loads(bounds_wkb, hex=True)
    if not (isinstance(bounds_geom, geometry.Polygon) or isinstance(bounds_geom, geometry.MultiPolygon)):
        raise ValueError(f"Encountered {bounds_geom.type} instead of Polygon or MultiPolygon type for bounds.")

    filter_classes = [
        "Airports",
        "Continuous urban fabric (S.L. : > 80%)",
        "Discontinuous dense urban fabric (S.L. : 50% -  80%)",
        "Discontinuous low density urban fabric (S.L. : 10% - 30%)",
        "Discontinuous medium density urban fabric (S.L. : 30% - 50%)",
        "Discontinuous very low density urban fabric (S.L. : < 10%)",
        "Green urban areas",
        "Industrial, commercial, public, military and private units",
        "Isolated structures",
        "Land without current use",
        "Port areas",
        "Sports and leisure facilities",
    ]

    # iter zip files and load if intersecting bounds
    dir_path: Path = Path(dir_path_str)
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
                        with fiona.open(full_gpkg_path) as src:
                            if not geometry.box(*src.bounds).intersects(bounds_geom):
                                continue
                        gdf = gpd.read_file(full_gpkg_path)
                        # discard rows if in filtered classes
                        gdf = gdf[gdf.class_2018.isin(filter_classes)]
                        # filter spatially
                        gdf["bbox"] = gdf["geometry"].envelope
                        gdf.set_geometry("bbox", inplace=True)
                        gdf_itx = gdf[gdf.intersects(bounds_geom)]
                        gdf_itx.rename(columns={"geometry": "geom", "Pop2018": "pop2018"}, inplace=True)
                        gdf_itx.set_geometry("geom", inplace=True)
                        # explode multipolygons
                        gdf_exp = gdf_itx.explode(index_parts=False)
                        # write to postgis
                        cols = [
                            "country",
                            "fua_name",
                            "fua_code",
                            "code_2018",
                            "class_2018",
                            "identifier",
                            "comment",
                            "pop2018",
                            "geom",
                        ]
                        gdf_exp[cols].to_postgis(
                            atlas_table_name,
                            engine,
                            if_exists="append",
                            schema=schema_name,
                            index=True,
                            index_label="temp_id",
                        )
            # Delete the unzipped files
            shutil.rmtree(unzip_dir)
    with engine.connect() as connection:
        connection.execute(  # type: ignore
            text(
                f"""
                ALTER TABLE {schema_name}.{atlas_table_name} ADD COLUMN fid serial;
                ALTER TABLE {schema_name}.{atlas_table_name} ADD PRIMARY KEY (fid);
                ALTER TABLE {schema_name}.{atlas_table_name} DROP COLUMN temp_id;
            """
            )
        )
        connection.commit()


if __name__ == "__main__":
    """
    Examples are run from the project folder (the folder containing src)
    python -m src.data.load_urban_atlas "./temp/urban atlas" eu bounds blocks
    """
    parser = argparse.ArgumentParser(description="Load building heights raster data.")
    parser.add_argument("data_dir_path", type=str, help="Input data directory with zipped data files.")
    parser.add_argument("schema_name", type=str, help="Schema name.")
    parser.add_argument("bounds_table_name", type=str, help="Table name for already loaded urban # boundaries.")
    parser.add_argument("atlas_table_name", type=str, help="Table name for urban atlas blocks data.")
    args = parser.parse_args()
    logger.info(f"Loading urban atlas blocks data from path: {args.data_dir_path}")
    data_dir_path = Path(args.data_dir_path)
    if not data_dir_path.exists():
        raise IOError("Input directory does not exist")
    if not data_dir_path.is_dir():
        raise IOError("Expected input directory, not a file name")
    load_urban_blocks(args.data_dir_path, args.schema_name, args.bounds_table_name, args.atlas_table_name)
    # load_urban_blocks("./temp/urban atlas", "eu", "bounds", "blocks")
