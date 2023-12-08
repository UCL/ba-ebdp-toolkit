# pyright: basic
import pytest
from shapely import geometry

from src import tools


def test_split_street_segments():
    """ """
    point_a = geometry.Point(0, 0)
    point_b = geometry.Point(0.2, 0.2)
    point_c = geometry.Point(0.8, 0.8)
    point_d = geometry.Point(1, 1)
    point_off = geometry.Point(1, 0)
    line_a = geometry.LineString([point_a, point_b])
    line_b = geometry.LineString([point_a, point_b, point_c])
    line_c = geometry.LineString([point_a, point_b, point_c, point_d])
    # simple case
    seg_pairs = tools.split_street_segment(line_a, [("a", point_a), ("b", point_b)])
    assert len(seg_pairs) == 1
    out_line, connector_a, connector_b = seg_pairs[0]
    assert out_line in [line_a, line_a.reverse()]
    assert connector_a[-1] in [point_a, point_b]
    assert connector_b[-1] in [point_a, point_b]
    # case with extraneous point
    seg_pairs = tools.split_street_segment(line_a, [("a", point_a), ("b", point_b), ("c", point_off)])
    assert len(seg_pairs) == 1
    out_line, connector_a, connector_b = seg_pairs[0]
    assert out_line in [line_a, line_a.reverse()]
    assert connector_a[-1] in [point_a, point_b]
    assert connector_b[-1] in [point_a, point_b]
    # case with one inside point
    seg_pairs = tools.split_street_segment(line_b, [("a", point_a), ("b", point_b), ("c", point_c)])
    assert len(seg_pairs) == 2
    # check that the pairings match
    seg_lines = set()
    for seg_line, seg_con_a, seg_con_b in seg_pairs:
        seg_lines.add(seg_line)
        _, out_point_a = seg_con_a
        _, out_point_b = seg_con_b
        assert seg_line.distance(out_point_a) == 0
        assert seg_line.distance(out_point_b) == 0
        assert out_point_a != out_point_b
    assert len(seg_lines) == 2
    seg_lines = list(seg_lines)
    assert seg_lines[0] not in [seg_lines[1], seg_lines[1].reverse()]
    # case with two inside points
    seg_pairs = tools.split_street_segment(line_c, [("a", point_a), ("b", point_b), ("c", point_c), ("d", point_d)])
    assert len(seg_pairs) == 3
    # check that the pairings match
    seg_lines = set()
    for seg_line, seg_con_a, seg_con_b in seg_pairs:
        seg_lines.add(seg_line)
        _, out_point_a = seg_con_a
        _, out_point_b = seg_con_b
        assert seg_line.distance(out_point_a) == 0
        assert seg_line.distance(out_point_b) == 0
        assert out_point_a != out_point_b
    assert len(seg_lines) == 3
    seg_lines = list(seg_lines)
    assert seg_lines[0] not in [seg_lines[1], seg_lines[1].reverse(), seg_lines[2], seg_lines[2].reverse()]
    assert seg_lines[1] not in [seg_lines[0], seg_lines[0].reverse(), seg_lines[2], seg_lines[2].reverse()]
    assert seg_lines[2] not in [seg_lines[1], seg_lines[1].reverse(), seg_lines[0], seg_lines[0].reverse()]


def test_prepare_schema():
    schema = tools.generate_overture_schema()
