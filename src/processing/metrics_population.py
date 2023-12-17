'''
from rasterio.enums import Resampling
from rasterio.io import MemoryFile
from rasterstats import point_query
from tqdm import tqdm

pop_raster = tools.db_fetch(
    f"""
    WITH bounds AS (
        SELECT geom
        FROM eu.bounds
    ), rasters AS (
        SELECT rast 
        FROM eu.pop_dens, bounds as b
        WHERE ST_Intersects(rast, b.geom)
    ), mosaic AS (
        SELECT ST_Union(rasters.rast) as merged 
        FROM rasters
    )
    SELECT ST_AsTiff(ST_Clip(m.merged, b.geom))
        FROM mosaic m, bounds as b
        LIMIT 1;
    """
)

upscale_factor = 10
with MemoryFile(pop_raster) as memfile:
    with memfile.open() as dataset:
        # Read the data
        data = dataset.read(
            out_shape=(dataset.count, int(dataset.height * upscale_factor), int(dataset.width * upscale_factor)),
            resampling=Resampling.bilinear,
        )
        # Update the transform to reflect the new shape
        old_trf = dataset.transform
        new_trf = old_trf * old_trf.scale((dataset.width / data.shape[-1]), (dataset.height / data.shape[-2]))
        for node_idx, node_row in tqdm(nodes_gdf.iterrows(), total=len(nodes_gdf)):
            pop_val = point_query(
                node_row["geom"],
                data,
                interpolate="nearest",
                affine=new_trf,
                nodata=np.nan,
            )
            nodes_gdf.loc[node_idx, "pop_dens"] = np.clip(pop_val, 0, np.inf)[0]
'''
