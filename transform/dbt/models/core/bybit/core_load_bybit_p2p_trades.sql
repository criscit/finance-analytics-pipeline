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
  amount,
  price,
  fee,
  type,
  status,
  executed_at,
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
