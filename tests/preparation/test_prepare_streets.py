# pyright: basic
import pytest

from src.preparation import prepare_streets


def test_generate_graph(london_box_poly):
    # prepare_streets.generate_graph("temp/nodes_small.gpkg", "temp/edges_small.gpkg", 27700, london_box_poly)
    prepare_streets.generate_graph("temp/nodes_eu.gpkg", "temp/edges_eu.gpkg", 4258)
