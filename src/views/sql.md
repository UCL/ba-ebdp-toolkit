# SQL

## Views

```sql
CREATE SCHEMA IF NOT EXISTS metrics_views;
```

Centrality

```sql
CREATE OR REPLACE VIEW metrics_views.centrality_168 AS
SELECT
    mc.*,
    ST_SetSRID(nnc.edge_geom, 3035)::geometry(Linestring, 3035) AS edge_geom
FROM
    overture.dual_nodes nnc
JOIN
    metrics.centrality mc
ON
    nnc.fid = mc.fid
WHERE bounds_fid = 168;
```
