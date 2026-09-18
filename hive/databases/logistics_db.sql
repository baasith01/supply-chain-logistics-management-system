-- Logistics & Supply Chain Intelligence Platform
-- Hive database definition
--
-- Purpose:
--   Creates the Hive database used by the project's analytical tables.
--
-- The database is intentionally separated from the default Hive database
-- so that project tables can be managed as one logical namespace.

CREATE DATABASE IF NOT EXISTS logistics_db
COMMENT 'Hive database for the Logistics and Supply Chain Intelligence Platform';

USE logistics_db;

-- Verify the active database:
-- SELECT current_database();

-- Project table groups created under this database:
--
-- orders
-- customers
-- drivers
-- vehicles
-- warehouses
-- gps_tracking
-- weather
-- traffic
--
-- Example:
-- SHOW TABLES;

-- The individual CREATE TABLE statements are maintained in:
--
-- hive/tables/
--   orders.sql
--   customers.sql
--   drivers.sql
--   vehicles.sql
--   warehouses.sql
--   gps_tracking.sql
--   weather.sql
--   traffic.sql

-- Recommended execution order:
--
-- 1. Create the database:
--      hive -f hive/databases/logistics_db.sql
--
-- 2. Execute the table definitions after selecting logistics_db:
--      USE logistics_db;
--
-- 3. Verify:
--      SHOW TABLES;
