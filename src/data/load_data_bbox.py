""" """

from __future__ import annotations

import argparse

from src.data import loaders
from src.tools import get_logger

logger = get_logger(__name__)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process min_x, min_y, max_x, and max_y.")
    parser.add_argument("min_x", default=-12.421470912798974, type=int, help="Minimum x value.")
    parser.add_argument("min_y", default=33.226730183416954, type=int, help="Minimum y value.")
    parser.add_argument("max_x", default=45.535158355759435, type=int, help="Maximum x value.")
    parser.add_argument("max_y", default=71.13547646352613, type=int, help="Maximum y value.")
    args = parser.parse_args()
    logger.info(f"Loading data for bbox: {args.min_x}, {args.min_y}, {args.max_x}, {args.max_y}")
    loaders.load_buildings(args.min_x, args.min_y, args.max_x, args.max_y)
    loaders.load_places(args.min_x, args.min_y, args.max_x, args.max_y)
    loaders.load_nodes(args.min_x, args.min_y, args.max_x, args.max_y)
    loaders.load_edges(args.min_x, args.min_y, args.max_x, args.max_y)
