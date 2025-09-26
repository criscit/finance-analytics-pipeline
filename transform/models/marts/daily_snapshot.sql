-- file: transform/models/marts/daily_snapshot.sql
{{ config(materialized='table', schema='marts') }}

with src as (
  select * from {{ ref('core_events') }}
),
snap as (
  select
    id,
    user_id,
    event_type,
    event_ts as updated_at,
    metric_1,
    metric_2
  from src
)
select * from snap
order by updated_at, id



