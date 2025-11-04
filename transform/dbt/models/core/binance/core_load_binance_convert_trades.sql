{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='convert_trade_bk'
) }}

select
  md5(
    concat_ws(
      '|',
      'binance_convert_trades',
      order_id
    )
  ) as convert_trade_bk,
  quote_id,
  create_time_ms,
  from_amt,
  from_asset,
  from_price_usd,
  to_amt,
  to_asset,
  to_price_usd,
  ratio,
  inverse_ratio,
  order_status,
  order_type,
  side,
  wallet_type,

  current_timestamp at time zone 'UTC' as processed_at
from
  {{ ref('stg_load_binance_convert_trades') }}
{% if is_incremental() %}
  where processed_at >= (
    select
      coalesce(max(processed_at), '1900-01-02'::timestamp) - interval '1 day'
    from
      {{ this }}
  )
{% endif %}
