"""
# GREEN
tree_cover_unary = tree_cover_gdf.geometry.unary_union.simplify(5)
green_space_unary = green_space_gdf.geometry.unary_union.simplify(5)

for dist in [400, 800, 1600]:
    logger.info(f"Processing distance: {dist}")
    # Buffer the nodes
    nodes_gdf[f"geom_{dist}"] = nodes_gdf["geom"].apply(lambda x: x.buffer(dist))
    for node_idx, node_row in tqdm(nodes_gdf.iterrows(), total=len(nodes_gdf)):
        # intersect with the unary geoms
        nodes_gdf.loc[node_idx, f"tree_cover_{dist}"] = node_row[f"geom_{dist}"].intersection(tree_cover_unary).area
        nodes_gdf.loc[node_idx, f"green_space_{dist}"] = node_row[f"geom_{dist}"].intersection(green_space_unary).area
    nodes_gdf.drop(columns=[f"geom_{dist}"], inplace=True)
"""
