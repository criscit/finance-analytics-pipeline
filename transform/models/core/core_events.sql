-- file: transform/models/core/core_events.sql
{{ config(materialized='table', schema='core') }}

with src as (
    select * from {{ ref('staging_events') }}
),
cleaned as (
    select
        id,
        user_id,
        event_type,
        event_ts,
        coalesce(metric_1, 0) as metric_1,
        coalesce(metric_2, 0) as metric_2,
        created_at
    from src
    where user_id is not null
      and event_type is not null
)
select * from cleaned



