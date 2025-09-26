-- file: transform/models/staging/stg_events.sql
{{ config(materialized='table', schema='staging') }}

select
    id,
    user_id,
    event_type,
    event_ts,
    metric_1,
    metric_2,
    created_at
from stg_events
