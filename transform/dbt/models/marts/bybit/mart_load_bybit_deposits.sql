{{ config(
    materialized='incremental',
    unique_key='deposit_bk'
) }}

select
  deposit_bk,
  amount,
  deposit_fee,
  chain,
  coin,
  price_usd,
  (to_timestamp(success_at_ms / 1000.0) at time zone 'Europe/Moscow') as success_at,
  date(to_timestamp(success_at_ms / 1000.0) at time zone 'Europe/Moscow') as success_dt,
  current_timestamp at time zone 'UTC' as processed_at
from
  {{ ref('core_load_bybit_deposits') }}
where
  status = 3
{% if is_incremental() %}
  and processed_at >= (
    select
      coalesce(max(processed_at), '1900-01-02'::timestamp) - interval '1 day'
    from
      {{ this }}
  )
  and deposit_bk not in (
    select deposit_bk from {{ this }}
  )
{% endif %}
