{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='deposit_bk'
) }}

select
  deposit_bk,
  amount,
  deposit_fee,
  chain,
  coin,
  (to_timestamp(success_at_ms / 1000.0) at time zone 'Europe/Moscow') as success_at,
  current_timestamp at time zone 'UTC' as processed_at
from
  {{ ref('core_load_bybit_deposits') }}
where
  status = 3
{% if is_incremental() %}
  and processed_at >= (
    select
      coalesce(max(processed_at), '1900-01-02'::timestamp) - interval '1 day'
    from
      {{ this }}
  )
{% endif %}
