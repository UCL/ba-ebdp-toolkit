""" """

from __future__ import annotations

import argparse
import threading
import time
from pathlib import Path

from data import overture_downloaders
from src.tools import get_logger

logger = get_logger(__name__)

if __name__ == "__main__":
    """
    Examples are run from the project folder (the folder containing src)

    For loading EU bbox
    python -m src.data.download_overture_bbox ./temp eu -12.4214 33.2267 45.5351 71.1354
    """
    parser = argparse.ArgumentParser(
        description="Process Overture maps data for min_x, min_y, max_x, and max_y bounds."
    )
    parser.add_argument("out_path", type=str, help="Output filepath.")
    parser.add_argument("file_prefix", type=str, help="Name prefix for output files.")
    parser.add_argument("min_x", type=float, help="Minimum x value.")
    parser.add_argument("min_y", type=float, help="Minimum y value.")
    parser.add_argument("max_x", type=float, help="Maximum x value.")
    parser.add_argument("max_y", type=float, help="Maximum y value.")
    args = parser.parse_args()
    logger.info(f"Loading data for bbox: {args.min_x}, {args.min_y}, {args.max_x}, {args.max_y}")
    out_path = Path(args.out_path)
    if not out_path.exists():
        raise IOError("Output path does not exist")
    if not out_path.is_dir():
        raise IOError("Expected output directory, not a file name")
    logger.info(f'Writing output files to "{out_path.resolve()}" using prefix of "{args.file_prefix}"')
    # run queries
    # space the threads starts otherwise they trip over each other with DuckDB in memory database initialisation
    load_buildings = threading.Thread(
        target=overture_downloaders.load_buildings,
        args=(out_path, args.file_prefix, args.min_x, args.min_y, args.max_x, args.max_y),
    )
    load_buildings.start()
    time.sleep(1)
    load_places = threading.Thread(
        target=overture_downloaders.load_places,
        args=(out_path, args.file_prefix, args.min_x, args.min_y, args.max_x, args.max_y),
    )
    load_places.start()
    time.sleep(1)
    load_nodes = threading.Thread(
        target=overture_downloaders.load_nodes,
        args=(out_path, args.file_prefix, args.min_x, args.min_y, args.max_x, args.max_y),
    )
    load_nodes.start()
    time.sleep(1)
    load_edges = threading.Thread(
        target=overture_downloaders.load_edges,
        args=(out_path, args.file_prefix, args.min_x, args.min_y, args.max_x, args.max_y),
    )
    load_edges.start()
    time.sleep(1)

    load_buildings.join()
    load_places.join()
    load_nodes.join()
    load_edges.join()
    logger.info(f"Completed load of {args.file_prefix} data")
