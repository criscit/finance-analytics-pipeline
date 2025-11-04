{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='earn_yield_bk'
) }}

select
  md5(
    concat_ws(
      '|',
      'bybit_earn_yield',
      id,
      cast(createdAt_ms as varchar)
    )
  ) as earn_yield_bk,
  amount,
  coin,
  to_timestamp(createdAt_ms / 1000.0) as created_at_utc,
  distributionMode,
  effectiveStakingAmount,
  id,
  orderId,
  price_usd,
  productId,
  status,
  yieldType,
  current_timestamp at time zone 'UTC' as processed_at
from
  {{ ref('stg_load_bybit_earn_yield') }}

{% if is_incremental() %}
  where processed_at >= (
    select
      coalesce(max(processed_at), '1900-01-02'::timestamp) - interval '1 day'
    from
      {{ this }}
  )
{% endif %}
