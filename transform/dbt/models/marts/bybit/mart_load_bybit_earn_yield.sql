{{ config(
    materialized='incremental',
    unique_key='earn_yield_bk'
) }}

select
  earn_yield_bk,
  amount,
  coin,
  created_at_utc,
  date(created_at_utc at time zone 'Europe/Moscow') as created_dt,
  effectiveStakingAmount,
  price_usd,
  productId,
  yieldType,
  current_timestamp at time zone 'UTC' as processed_at
from
  {{ ref('core_load_bybit_earn_yield') }}
where
  status = 'Success'

{% if is_incremental() %}
  and processed_at >= (
    select
      coalesce(max(processed_at), '1900-01-02'::timestamp) - interval '1 day'
    from
      {{ this }}
  )
  and earn_yield_bk not in (
    select earn_yield_bk from {{ this }}
  )
{% endif %}
