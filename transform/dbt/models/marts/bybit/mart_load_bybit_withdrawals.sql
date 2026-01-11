{{ config(
    materialized='incremental',
    unique_key='withdrawal_bk'
) }}

select
  withdrawal_bk,
  amount,
  withdraw_fee,
  chain,
  coin,
  price_usd,
  executed_at_utc,
  withdraw_type,
  current_timestamp at time zone 'UTC' as processed_at
from
  {{ ref('core_load_bybit_withdrawals') }}
where
  status = 'success'
{% if is_incremental() %}
  and processed_at >= (
    select
      coalesce(max(processed_at), '1900-01-02'::timestamp) - interval '1 day'
    from
      {{ this }}
  )
  and withdrawal_bk not in (
    select withdrawal_bk from {{ this }}
  )
{% endif %}
