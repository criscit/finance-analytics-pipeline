{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='convert_trade_bk'
) }}

select
  convert_trade_bk,
  (to_timestamp(created_time_ms / 1000.0) at time zone 'Europe/Moscow') as created_at,
  exchange_rate,
  from_amt,
  from_coin,
  to_amt,
  to_coin,
  current_timestamp at time zone 'UTC' as processed_at
from
  {{ ref('core_load_bybit_convert_trades') }}

{% if is_incremental() %}
  where processed_at >= (
    select
      coalesce(max(processed_at), '1900-01-02'::timestamp) - interval '1 day'
    from
      {{ this }}
  )
{% endif %}
