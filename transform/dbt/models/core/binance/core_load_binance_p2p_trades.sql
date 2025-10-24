{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='p2p_trade_bk'
) }}

select
  md5(
    concat_ws(
      '|',
      'binance_p2p_trades',
      order_number
    )
  ) as p2p_trade_bk,
  adv_no,
  create_time_ms,
  additional_kyc_verify,
  amount,
  commission,
  taker_amt,
  taker_commission,
  taker_commission_rate,
  total_price,
  unit_price,
  asset,
  counterparty_nickname,
  fiat,
  fiat_symbol,
  order_status,
  pay_method_name,
  trade_type,

  current_timestamp at time zone 'UTC' as processed_at
from
  {{ ref('stg_load_binance_p2p_trades') }}
{% if is_incremental() %}
  where processed_at >= (
    select
      coalesce(max(processed_at), '1900-01-02'::timestamp) - interval '1 day'
    from
      {{ this }}
  )
{% endif %}
