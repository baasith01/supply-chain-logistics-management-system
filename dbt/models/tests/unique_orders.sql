-- ============================================================
-- dbt custom test: unique_orders
-- Logistics & Supply Chain Intelligence
-- ============================================================
--
-- Purpose:
--   Verify that the delivery mart contains at most one row per
--   order_id.
--
-- dbt custom-test rule:
--   A SQL test passes when this query returns ZERO rows.
--
-- Expected result:
--   No duplicate order_id values.
--
-- Usage:
--   dbt test --select unique_orders
--
-- This test targets delivery_mart because its documented grain is
-- one row per order_id.
-- ============================================================

with duplicate_orders as (

    select
        order_id,
        count(*) as row_count

    from {{ ref('delivery_mart') }}

    group by order_id

    having count(*) > 1

)

select
    order_id,
    row_count

from duplicate_orders
