-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- disable background workers to make results reproducible
\c :TEST_DBNAME :ROLE_SUPERUSER
SELECT _timescaledb_internal.stop_background_workers();
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

CREATE TABLE metrics(f1 int, f2 int, time timestamptz NOT NULL, device_id int, value float);
SELECT create_hypertable('metrics','time');
ALTER TABLE metrics DROP COLUMN f1;

INSERT INTO metrics(time, device_id, value) SELECT '2000-01-01'::timestamptz, 1, 0.5;
INSERT INTO metrics(time, device_id, value) SELECT '2000-01-01'::timestamptz, 2, 0.5;
INSERT INTO metrics(time, device_id, value) SELECT '2000-01-01'::timestamptz, 3, 0.5;

--
-- test switching continuous agg view between different modes
--

-- check default view for new continuous aggregate
CREATE VIEW metrics_summary WITH (timescaledb.continuous) AS SELECT time_bucket('1d',time), avg(value) FROM metrics GROUP BY 1;
ALTER TABLE metrics DROP COLUMN f2;

-- this should be union view
SELECT pg_get_viewdef('metrics_summary',true);
SELECT time_bucket,avg FROM metrics_summary ORDER BY 1;

-- downgrade view to non-union view
ALTER VIEW metrics_summary SET (timescaledb.materialized_only=true);
-- this should be view without union
SELECT pg_get_viewdef('metrics_summary',true);
SELECT time_bucket,avg FROM metrics_summary ORDER BY 1;

-- upgrade view to union view again
ALTER VIEW metrics_summary SET (timescaledb.materialized_only=false);
-- this should be union view
SELECT pg_get_viewdef('metrics_summary',true);
SELECT time_bucket,avg FROM metrics_summary ORDER BY 1;

-- try upgrade view to union view that is already union view
ALTER VIEW metrics_summary SET (timescaledb.materialized_only=false);
-- this should be union view
SELECT pg_get_viewdef('metrics_summary',true);
SELECT time_bucket,avg FROM metrics_summary ORDER BY 1;

-- refresh
REFRESH MATERIALIZED VIEW metrics_summary;
-- result should not change after refresh for union view
SELECT time_bucket,avg FROM metrics_summary ORDER BY 1;

-- downgrade view to non-union view
ALTER VIEW metrics_summary SET (timescaledb.materialized_only=true);
-- this should be view without union
SELECT pg_get_viewdef('metrics_summary',true);
-- view should have results now after refresh
SELECT time_bucket,avg FROM metrics_summary ORDER BY 1;

DROP VIEW metrics_summary CASCADE;

-- check default view for new continuous aggregate with materialized_only to true
CREATE VIEW metrics_summary WITH (timescaledb.continuous, timescaledb.materialized_only=true) AS SELECT time_bucket('1d',time), avg(value) FROM metrics GROUP BY 1;

-- this should be view without union
SELECT pg_get_viewdef('metrics_summary',true);
SELECT time_bucket,avg FROM metrics_summary ORDER BY 1;

-- upgrade view to union view
ALTER VIEW metrics_summary SET (timescaledb.materialized_only=false);
-- this should be union view
SELECT pg_get_viewdef('metrics_summary',true);
SELECT time_bucket,avg FROM metrics_summary ORDER BY 1;

-- downgrade view to non-union view
ALTER VIEW metrics_summary SET (timescaledb.materialized_only=true);
-- this should be view without union
SELECT pg_get_viewdef('metrics_summary',true);
SELECT time_bucket,avg FROM metrics_summary ORDER BY 1;

DROP VIEW metrics_summary CASCADE;

--
-- test queries on union view
--

CREATE VIEW metrics_summary WITH (timescaledb.continuous, timescaledb.materialized_only=true) AS SELECT time_bucket('1d',time), avg(value) FROM metrics GROUP BY 1;

-- query should not have results since cagg is materialized only and no refresh has happened yet
SELECT time_bucket,avg FROM metrics_summary ORDER BY 1;

ALTER VIEW metrics_summary SET (timescaledb.materialized_only=false);

-- after switch to union view all results should be returned
SELECT time_bucket,avg FROM metrics_summary ORDER BY 1;

REFRESH MATERIALIZED VIEW metrics_summary;
ALTER VIEW metrics_summary SET (timescaledb.materialized_only=true);

-- materialized only view should return data now too because refresh has happened
SELECT time_bucket,avg FROM metrics_summary ORDER BY 1;

-- add some more data
INSERT INTO metrics(time, device_id, value) SELECT '2000-02-01'::timestamptz, 1, 0.1;
INSERT INTO metrics(time, device_id, value) SELECT '2000-02-01'::timestamptz, 2, 0.2;
INSERT INTO metrics(time, device_id, value) SELECT '2000-02-01'::timestamptz, 3, 0.3;

-- materialized only view should not have new data yet
SELECT time_bucket,avg FROM metrics_summary ORDER BY 1;

-- but union view should
ALTER VIEW metrics_summary SET (timescaledb.materialized_only=false);
SELECT time_bucket,avg FROM metrics_summary ORDER BY 1;

-- and after refresh non union view should have new data too
REFRESH MATERIALIZED VIEW metrics_summary;
ALTER VIEW metrics_summary SET (timescaledb.materialized_only=true);
SELECT time_bucket,avg FROM metrics_summary ORDER BY 1;

