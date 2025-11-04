{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='withdrawal_bk'
) }}

select
  md5(
    concat_ws(
      '|',
      'bybit_withdrawals',
      withdraw_id
    )
  ) as withdrawal_bk,
  amount,
  tax,
  tax_rate,
  withdraw_fee,
  chain,
  coin,
  price_usd,
  create_time_ms,
  update_time_ms,
  status,
  tag,
  tax_type,
  to_address,
  tx_id,
  withdraw_type,

  current_timestamp at time zone 'UTC' as processed_at
from
  {{ ref('stg_load_bybit_withdrawals') }}
{% if is_incremental() %}
  where processed_at >= (
    select
      coalesce(max(processed_at), '1900-01-02'::timestamp) - interval '1 day'
    from
      {{ this }}
  )
{% endif %}
