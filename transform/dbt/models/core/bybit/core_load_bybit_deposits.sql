{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='deposit_bk'
) }}

select
  md5(
    concat_ws(
      '|',
      'bybit_deposits',
      id
    )
  ) as deposit_bk,
  amount,
  batch_release_limit,
  deposit_fee,
  block_hash,
  chain,
  coin,
  price_usd,
  confirmations,
  deposit_type,
  from_address,
  status,
  to_timestamp(success_at_ms / 1000.0) as success_at_utc,
  tag,
  tax_deposit_records_id,
  tax_status,
  to_address,
  tx_id,
  tx_index,

  current_timestamp at time zone 'UTC' as processed_at
from
  {{ ref('stg_load_bybit_deposits') }}
{% if is_incremental() %}
  where processed_at >= (
    select
      coalesce(max(processed_at), '1900-01-02'::timestamp) - interval '1 day'
    from
      {{ this }}
  )
{% endif %}
