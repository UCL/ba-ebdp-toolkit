REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public, eu, loads, metrics, overture FROM viewer;
GRANT USAGE ON SCHEMA public, eu, metrics, overture TO viewer;
GRANT SELECT ON ALL TABLES IN SCHEMA public, eu, metrics, overture TO viewer;
ALTER DEFAULT PRIVILEGES IN SCHEMA public, eu, metrics, overture GRANT SELECT ON TABLES TO viewer;