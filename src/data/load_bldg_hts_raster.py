""" """

import argparse
import os
import shutil
import subprocess
import zipfile
from pathlib import Path

from src import tools

logger = tools.get_logger(__name__)
db_config = tools.get_db_config()
os.environ["PGPASSWORD"] = db_config["password"]  # type: ignore


def load_bldg_hts(data_dir_path: str, bin_path: str | None) -> None:
    """ """
    # drop existing
    tools.drop_table("eu", "bldg_hts")
    # Loop through each ZIP file and upload TIFs
    first_file = True
    dir_path: Path = Path(data_dir_path)
    unzip_dir = dir_path / "temp_unzipped/"
    for zip_file_name in os.listdir(dir_path):
        if zip_file_name.endswith(".zip"):
            # Create directory for unzipped files
            if os.path.exists(unzip_dir):
                shutil.rmtree(unzip_dir)
            os.makedirs(unzip_dir)
            full_zip_path = dir_path / zip_file_name
            # Unzip
            with zipfile.ZipFile(full_zip_path, "r") as zip_ref:
                zip_ref.extractall(unzip_dir)
            # Find the tif file
            for walk_dir_path, _dir_names, file_names in os.walk(unzip_dir):
                for raster_file_name in file_names:
                    if raster_file_name.endswith(".tif"):
                        full_raster_path = str((Path(walk_dir_path) / raster_file_name).resolve())
                        sql_path = str((unzip_dir / "output.sql").resolve())
                        # Run raster2pgsql and psql to import the raster into PostGIS
                        try:
                            with open(sql_path, "w") as f:
                                subprocess.run(
                                    [
                                        "raster2pgsql" if bin_path is None else str(Path(bin_path) / "raster2pgsql"),
                                        "-c" if first_file is True else "-a",
                                        "-s",
                                        "3035",
                                        full_raster_path,
                                        "eu.bldg_hts",
                                    ],
                                    check=True,
                                    stdout=f,
                                )
                            subprocess.run(
                                [  # type: ignore
                                    "psql" if bin_path is None else str(Path(bin_path) / "psql"),
                                    "-h",
                                    db_config["host"],
                                    "-U",
                                    db_config["user"],
                                    "-d",
                                    db_config["database"],
                                    "-p",
                                    str(db_config["port"]),
                                    "-f",
                                    sql_path,
                                ],
                                check=True,
                            )
                        except Exception as err:
                            logger.error(err)
                            continue
            # Delete the unzipped files
            shutil.rmtree(unzip_dir)
            first_file = False
    # add constraints
    tools.db_execute(
        """
        SELECT AddRasterConstraints(
            'eu'::name, 
            'bldg_hts'::name, 
            'rast'::name,
            'blocksize',
            'extent',
            'num_bands',
            'pixel_types',
            'srid'
        );
        CREATE INDEX bldg_hts_rast_gist_idx
            ON eu.bldg_hts
            USING gist (ST_ConvexHull(rast));
        """
    )


if __name__ == "__main__":
    """
    Examples are run from the project folder (the folder containing src)
    python -m src.data.load_bldg_hts_raster "./temp/Digital height Model EU" eu bldg_hts \
        /Applications/Postgres.app/Contents/Versions/15/bin/
    """
    if True:
        parser = argparse.ArgumentParser(description="Load building heights raster data.")
        parser.add_argument("data_dir_path", type=str, help="Input data directory with zipped data files.")
        parser.add_argument(
            "--bin_path", type=str, required=False, default=None, help="Optional 'bin' path for raster2pgsql and psql."
        )
        args = parser.parse_args()
        logger.info(f"Loading building heights data from path: {args.data_dir_path}")
        data_dir_path = Path(args.data_dir_path)
        if not data_dir_path.exists():
            raise OSError("Input directory does not exist")
        if not data_dir_path.is_dir():
            raise OSError("Expected input directory, not a file name")
        load_bldg_hts(args.data_dir_path, args.bin_path)
    else:
        load_bldg_hts("./temp/Digital Height Model EU", "/Applications/Postgres.app/Contents/Versions/16/bin/")
