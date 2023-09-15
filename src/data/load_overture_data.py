"""
DuckDB doesn't set projection so set manually; so, if wanted, these need to be set manually with `ogr2ogr`, for example:

```bash
ogr2ogr -progress -a_srs EPSG:4326 temp/places_4326.gpkg temp/places.gpkg
```

`ogr2ogr` can likewise be used if clipping the datasets, for example:

```bash
ogr2ogr -progress -spat -12.421470912798974 33.226730183416954 45.535158355759435 71.13547646352613 temp/places_eu.gpkg temp/places_4326.gpkg
```

"""
from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import geopandas as gpd
from shapely import geometry, wkb

from src import tools

logger = tools.get_logger(__name__)
# iter bounds
# clip downloads
# upload


def snip_extents(
    path: str,
    extents_geom: geometry.Polygon,
    bin_path: str | None = None,
) -> Path:
    """ """
    # prepare paths
    input_path = Path(path)
    if not str(input_path).endswith(".gpkg"):
        raise ValueError(f'Expected file with extension of ".gpkg": {input_path}')
    if not input_path.exists():
        raise ValueError(f"Input path does not exist: {input_path}")
    staging_dir = input_path.parent / "temp_snip"
    if staging_dir.exists():
        shutil.rmtree(staging_dir)
    os.makedirs(staging_dir)
    snip_path = staging_dir / f"snip_{input_path.name}"
    # snip
    # ogr2ogr -progress -spat -12.421470912798974 33.226730183416954 45.535158355759435 71.13547646352613 temp/places_eu.gpkg temp/places_4326.gpkg
    subprocess.run(
        [  # type: ignore
            "ogr2ogr" if bin_path is None else str(Path(bin_path) / "ogr2ogr"),
            "-spat",
            str(extents_geom.bounds[0]),
            str(extents_geom.bounds[1]),
            str(extents_geom.bounds[2]),
            str(extents_geom.bounds[3]),
            str(snip_path),
            str(input_path),
        ],
        check=True,
    )
    return snip_path


def process_bounds(
    bounds_fid: str,
    overture_nodes_path: str,
    overture_edges_path: str,
    overture_places_path: str,
    overture_buildings_path: str,
    schema_name: str,
    bounds_table_name: str,
    bin_path: str | None = None,
) -> None:
    """ """
    # get input extents
    bounds_extents_10000 = tools.db_fetch(
        f"""
        SELECT ST_Envelope(ST_Transform(ST_Buffer(geom, 10000), 4326))
            FROM {schema_name}.{bounds_table_name}
            WHERE fid = {bounds_fid}
        """
    )[0][0]
    extents_geom_large: geometry.Polygon = wkb.loads(bounds_extents_10000)  # type: ignore
    nodes_path = snip_extents(overture_nodes_path, extents_geom_large, bin_path)
    nodes_gdf = gpd.read_file(nodes_path)
    nodes_gdf.set_index("id", inplace=True)
    nodes_gdf.rename(columns={"geometry": "geom"}, inplace=True)
    nodes_gdf.set_geometry("geom", inplace=True)
    nodes_gdf = nodes_gdf[nodes_gdf["geom"].apply(lambda x: x.within(extents_geom_large))]
    print("here")


if __name__ == "__main__":
    """ """
    bounds_fid = "639"
    overture_nodes_path = "temp/eu_nodes.gpkg"
    overture_edges_path = "temp/eu_edges.gpkg"
    overture_places_path = "temp/eu_places.gpkg"
    overture_buildings_path = "temp/eu_buildings.gpkg"
    process_bounds(
        bounds_fid,
        overture_nodes_path,
        overture_edges_path,
        overture_places_path,
        overture_buildings_path,
        "eu",
        "bounds",
    )
