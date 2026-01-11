{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='p2p_trade_bk'
) }}

select
  md5(
    concat_ws(
      '|',
      'bybit_p2p_trades',
      order_no
    )
  ) as p2p_trade_bk,
  qty,
  cast(regexp_extract(qty, '([0-9.]+)', 1) as double) as crypto_amt,
  regexp_extract(qty, '[A-Z]+$', 0) as crypto_currency,
  amount,
  cast(regexp_extract(amount, '([0-9.]+)', 1) as double) as fiat_amt,
  regexp_extract(amount, '[A-Z]+$', 0) as fiat_currency,
  price,
  cast(regexp_extract(price, '([0-9.]+)', 1) as double) as price_usd,
  fee,
  cast(regexp_extract(fee, '([0-9.]+)', 1) as double) as fee_amount,
  regexp_extract(fee, '[A-Z]+$', 0) as fee_currency,
  type,
  status,
  executed_at - interval '3 hours' as executed_at_utc,
  current_timestamp at time zone 'UTC' as processed_at
from
  {{ ref('stg_load_bybit_p2p_trades') }}
{% if is_incremental() %}
  where processed_at >= (
    select
      coalesce(max(processed_at), '1900-01-02'::timestamp) - interval '1 day'
    from
      {{ this }}
  )
{% endif %}
