{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='withdrawal_bk'
) }}

select
  withdrawal_bk,
  amount,
  withdraw_fee,
  chain,
  coin,
  (to_timestamp(create_time_ms / 1000.0) at time zone 'Europe/Moscow') as created_at,
  (to_timestamp(update_time_ms / 1000.0) at time zone 'Europe/Moscow') as updated_at,
  withdraw_type,
  current_timestamp at time zone 'UTC' as processed_at
from
  {{ ref('core_load_bybit_withdrawals') }}
where
  status = 'success'
{% if is_incremental() %}
  and processed_at >= (
    select
      coalesce(max(processed_at), '1900-01-02'::timestamp) - interval '1 day'
    from
      {{ this }}
  )
{% endif %}
