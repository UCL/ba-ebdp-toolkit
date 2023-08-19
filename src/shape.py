"""
THIS IS TEMPORARY
HANDLES MULTIPOLYGONS FOR Squareness AND CentroidCorners
CAN BE REMOVED IF MOMEPY MERGES PULL REQUEST: https://github.com/pysal/momepy/pull/507
"""
import numpy as np
import pandas as pd
from shapely.geometry import Point
from tqdm.auto import tqdm  # progress bar

__all__ = [
    "Squareness",
    "CentroidCorners",
]


class Squareness:
    """
    Calculates the squareness of each object in a given GeoDataFrame. Uses only
    external shape (``shapely.geometry.exterior``), courtyards are not included.
    Returns ``np.nan`` for MultiPolygons if containing multiple geoms.

    .. math::
        \\mu=\\frac{\\sum_{i=1}^{N} d_{i}}{N}

    where :math:`d` is the deviation of angle of corner :math:`i` from 90 degrees.

    Adapted from :cite:`dibble2017`.

    Parameters
    ----------
    gdf : GeoDataFrame
        A GeoDataFrame containing objects.
    verbose : bool (default True)
        If ``True``, shows progress bars in loops and indication of steps.

    Attributes
    ----------
    series : Series
        A Series containing resulting values.
    gdf : GeoDataFrame
        The original GeoDataFrame.

    Examples
    --------
    >>> buildings_df['squareness'] = momepy.Squareness(buildings_df).series
    100%|██████████| 144/144 [00:01<00:00, 129.49it/s]
    >>> buildings_df.squareness[0]
    3.7075816043359864
    """

    def __init__(self, gdf, verbose=True):
        self.gdf = gdf
        # define empty list for results
        results_list = []

        def _angle(a, b, c):
            ba = a - b
            bc = c - b

            cosine_angle = np.dot(ba, bc) / (np.linalg.norm(ba) * np.linalg.norm(bc))
            angle = np.degrees(np.arccos(cosine_angle))

            return angle

        def _calc(geom):
            angles = []
            points = list(geom.exterior.coords)  # get points of a shape
            if len(points) < 3:
                return np.nan
            stop = len(points) - 1
            for i in range(1, len(points)):  # for every point, calculate angle and add 1 if True angle
                a = np.asarray(points[i - 1])
                b = np.asarray(points[i])
                # in last case, needs to wrap around start to find finishing angle
                c = np.asarray(points[i + 1]) if i != stop else np.asarray(points[1])
                ang = _angle(a, b, c)
                if ang <= 175 or ang >= 185:
                    angles.append(ang)
                else:
                    continue
            deviations = [abs(90 - i) for i in angles]
            return np.mean(deviations)

        # fill new column with the value of area, iterating over rows one by one
        for geom in tqdm(gdf.geometry, total=gdf.shape[0], disable=not verbose):
            if geom.geom_type == "Polygon" or (geom.geom_type == "MultiPolygon" and len(geom.geoms) == 1):
                # unpack multis with single geoms
                if geom.geom_type == "MultiPolygon":
                    geom = geom.geoms[0]
                results_list.append(_calc(geom))
            else:
                results_list.append(np.nan)

        self.series = pd.Series(results_list, index=gdf.index)


class CentroidCorners:
    """
    Calculates the mean distance centroid - corners and standard deviation.
    Returns ``np.nan`` for MultiPolygons if containing multiple geoms.

    .. math::
        \\overline{x}=\\frac{1}{n}\\left(\\sum_{i=1}^{n} dist_{i}\\right);
        \\space \\mathrm{SD}=\\sqrt{\\frac{\\sum|x-\\overline{x}|^{2}}{n}}

    Adapted from :cite:`schirmer2015` and :cite:`cimburova2017`.

    Parameters
    ----------
    gdf : GeoDataFrame
        A GeoDataFrame containing objects.
    verbose : bool (default True)
        If ``True``, shows progress bars in loops and indication of steps.

    Attributes
    ----------
    mean : Series
        A Series containing mean distance values.
    std : Series
        A Series containing standard deviation values.
    gdf : GeoDataFrame
        The original GeoDataFrame.

    Examples
    --------
    >>> ccd = momepy.CentroidCorners(buildings_df)
    100%|██████████| 144/144 [00:00<00:00, 846.58it/s]
    >>> buildings_df['ccd_means'] = ccd.means
    >>> buildings_df['ccd_stdev'] = ccd.std
    >>> buildings_df['ccd_means'][0]
    15.961531913184833
    >>> buildings_df['ccd_stdev'][0]
    3.0810634305400177
    """

    def __init__(self, gdf, verbose=True):
        self.gdf = gdf
        # define empty list for results
        results_list = []
        results_list_sd = []

        # calculate angle between points, return true or false if real corner
        def true_angle(a, b, c):
            ba = a - b
            bc = c - b

            cosine_angle = np.dot(ba, bc) / (np.linalg.norm(ba) * np.linalg.norm(bc))
            angle = np.arccos(cosine_angle)

            if np.degrees(angle) <= 170:
                return True
            if np.degrees(angle) >= 190:
                return True
            return False

        def _calc(geom):
            distances = []  # set empty list of distances
            centroid = geom.centroid  # define centroid
            points = list(geom.exterior.coords)  # get points of a shape
            stop = len(points) - 1  # define where to stop
            for i in range(1, len(points)):  # for every point, calculate angle and add 1 if True angle
                a = np.asarray(points[i - 1])
                b = np.asarray(points[i])
                # in last case, needs to wrap around start to find finishing angle
                c = np.asarray(points[i + 1]) if i != stop else np.asarray(points[1])
                p = Point(points[i])
                # calculate distance point - centroid
                if true_angle(a, b, c) is True:
                    distances.append(centroid.distance(p))
                else:
                    continue
            return distances

        # iterating over rows one by one
        for geom in tqdm(gdf.geometry, total=gdf.shape[0], disable=not verbose):
            if geom.geom_type == "Polygon" or (geom.geom_type == "MultiPolygon" and len(geom.geoms) == 1):
                # unpack multis with single geoms
                if geom.geom_type == "MultiPolygon":
                    geom = geom.geoms[0]
                distances = _calc(geom)
                # circular buildings
                if not distances:
                    # handle z dims
                    coords = [(coo[0], coo[1]) for coo in geom.convex_hull.exterior.coords]
                    results_list.append(np.nan)  # not replicating _circle_radius from momepy for temp file
                    results_list_sd.append(0)
                # calculate mean and std dev
                else:
                    results_list.append(np.mean(distances))
                    results_list_sd.append(np.std(distances))
            else:
                results_list.append(np.nan)
                results_list_sd.append(np.nan)

        self.mean = pd.Series(results_list, index=gdf.index)
        self.std = pd.Series(results_list_sd, index=gdf.index)
