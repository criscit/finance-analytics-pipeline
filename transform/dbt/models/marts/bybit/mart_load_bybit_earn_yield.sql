{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='earn_yield_bk'
) }}

select
  earn_yield_bk,
  amount,
  coin,
  created_at_utc,
  distributionMode,
  effectiveStakingAmount,
  id,
  orderId,
  productId,
  status,
  yieldType,
  current_timestamp at time zone 'UTC' as processed_at
from
  {{ ref('core_load_bybit_earn_yield') }}

{% if is_incremental() %}
  where processed_at >= (
    select
      coalesce(max(processed_at), '1900-01-02'::timestamp) - interval '1 day'
    from
      {{ this }}
  )
{% endif %}
