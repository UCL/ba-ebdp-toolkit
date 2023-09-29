# pyright: basic
import pytest
from shapely import geometry


@pytest.fixture
def london_box_poly() -> geometry.Polygon:
    """ """
    return geometry.box(504295, 153074, 557543, 202770)
