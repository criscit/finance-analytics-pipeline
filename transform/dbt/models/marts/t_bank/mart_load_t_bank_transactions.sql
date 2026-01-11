{{ config(
    materialized='incremental',
    unique_key='transaction_bk'
) }}

select
  transaction_bk,
  category_nm,
  description,
  transaction_amt,
  transaction_currency_cd,
  transacted_at_utc,
  total_rewards_amt,
  current_timestamp at time zone 'UTC' as processed_at
from
  {{ ref('core_load_t_bank_transactions') }}
where
  status_nm = 'OK'

{% if is_incremental() %}
  and processed_at >= (
    select
      coalesce(max(processed_at), '1900-01-02'::timestamp) - interval '1 day'
    from
      {{ this }}
  )
  and transaction_bk not in (
    select transaction_bk from {{ this }}
  )
{% endif %}
