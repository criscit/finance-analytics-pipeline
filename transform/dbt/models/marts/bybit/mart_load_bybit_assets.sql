{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='asset_bk'
) }}

select
  asset_bk,
  coin,
  equity,
  free,
  locked,
  usd_value,
  current_timestamp at time zone 'UTC' as processed_at
from
  {{ ref('core_load_bybit_assets') }}

{% if is_incremental() %}
  where processed_at >= (
    select
      coalesce(max(processed_at), '1900-01-02'::timestamp) - interval '1 day'
    from
      {{ this }}
  )
{% endif %}
