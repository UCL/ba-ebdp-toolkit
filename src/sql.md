# SQL

## Views

```sql
CREATE SCHEMA IF NOT EXISTS metrics_views;
```

Centrality

```sql
CREATE OR REPLACE VIEW metrics_views.centrality AS
SELECT
    mc.*,
    nnc.edge_geom
FROM
    overture.network_nodes_clean nnc
JOIN
    metrics.centrality mc
ON
    nnc.fid = mc.fid;
```
