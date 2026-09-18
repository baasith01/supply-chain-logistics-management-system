-- ============================================================
-- dbt custom test: valid_delivery_status
-- Logistics & Supply Chain Intelligence
-- ============================================================
--
-- Purpose:
--   Verify that delivery_status values in the delivery mart are
--   within the project's accepted operational status vocabulary.
--
-- dbt custom-test rule:
--   A SQL test passes when this query returns ZERO rows.
--
-- Accepted values:
--   delivered
--   completed
--   pending
--   in_transit
--   cancelled
--   failed
--   returned
--   out_for_delivery
--
-- The test is case-insensitive and trims surrounding whitespace.
--
-- Usage:
--   dbt test --select valid_delivery_status
--
-- Note:
--   If the source system introduces a legitimate new status,
--   update this controlled vocabulary before running production
--   validation.
-- ============================================================

with invalid_statuses as (

    select
        delivery_status,
        count(*) as row_count

    from {{ ref('delivery_mart') }}

    where delivery_status is not null

      and lower(trim(delivery_status)) not in (
          'delivered',
          'completed',
          'pending',
          'in_transit',
          'cancelled',
          'failed',
          'returned',
          'out_for_delivery'
      )

    group by delivery_status

)

select
    delivery_status,
    row_count

from invalid_statuses
