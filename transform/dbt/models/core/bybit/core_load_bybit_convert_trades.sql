{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='convert_trade_bk'
) }}

select
  md5(
    concat_ws(
      '|',
      'bybit_convert_trades',
      exchange_tx_id
    )
  ) as convert_trade_bk,
  created_time_ms,
  exchange_rate,
  from_amt,
  from_coin,
  from_price_usd,
  to_amt,
  to_coin,
  to_price_usd,
  current_timestamp at time zone 'UTC' as processed_at
from
  {{ ref('stg_load_bybit_convert_trades') }}

{% if is_incremental() %}
  where processed_at >= (
    select
      coalesce(max(processed_at), '1900-01-02'::timestamp) - interval '1 day'
    from
      {{ this }}
  )
{% endif %}
