{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='transfer_bk'
) }}

select
  transfer_bk,
  amount,
  coin,
  price_usd,
  from_account_type,
  to_account_type,
  (to_timestamp(timestamp_ms / 1000.0) at time zone 'Europe/Moscow') as success_at,
  date(to_timestamp(timestamp_ms / 1000.0) at time zone 'Europe/Moscow') as success_dt,
  current_timestamp at time zone 'UTC' as processed_at
from
  {{ ref('core_load_bybit_internal_transfers') }}
where
  status = 'SUCCESS'
{% if is_incremental() %}
  and processed_at >= (
    select
      coalesce(max(processed_at), '1900-01-02'::timestamp) - interval '1 day'
    from
      {{ this }}
  )
{% endif %}
