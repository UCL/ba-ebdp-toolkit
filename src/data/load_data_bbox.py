""" """

from __future__ import annotations

import argparse
from pathlib import Path

from src.data import loaders
from src.tools import get_logger

logger = get_logger(__name__)

if __name__ == "__main__":
    """
    Examples are run from the project folder (the folder containing src)

    For loading EU bbox
    python -m src.data.load_data_bbox ./temp eu -12.4214 33.2267 45.5351 71.1354

    Example for smaller extents, e.g. Athens
    python -m src.data.load_data_bbox ./temp athens 23.5564 37.7753 24.0717 38.1758
    """
    parser = argparse.ArgumentParser(description="Process min_x, min_y, max_x, and max_y.")
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
    logger.info(f'Writing output files to "{out_path.resolve()}" using prefix of "{args.file_prefix}"')
    loaders.load_buildings(out_path, args.file_prefix, args.min_x, args.min_y, args.max_x, args.max_y)
    loaders.load_places(out_path, args.file_prefix, args.min_x, args.min_y, args.max_x, args.max_y)
    loaders.load_nodes(out_path, args.file_prefix, args.min_x, args.min_y, args.max_x, args.max_y)
    loaders.load_edges(out_path, args.file_prefix, args.min_x, args.min_y, args.max_x, args.max_y)
