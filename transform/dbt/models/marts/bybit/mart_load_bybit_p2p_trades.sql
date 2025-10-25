{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='p2p_trade_bk'
) }}

select
  p2p_trade_bk,
  qty,
  amount,
  price,
  fee,
  type,
  executed_at as executed_at_utc,
  (executed_at at time zone 'UTC') at time zone 'Europe/Moscow' as executed_at_msk,
  current_timestamp at time zone 'UTC' as processed_at
from
  {{ ref('core_load_bybit_p2p_trades') }}
where
  status = 'Completed'
{% if is_incremental() %}
  and processed_at >= (
    select
      coalesce(max(processed_at), '1900-01-02'::timestamp) - interval '1 day'
    from
      {{ this }}
  )
{% endif %}
