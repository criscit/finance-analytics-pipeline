{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='transaction_bk'
) }}

select
  transaction_bk,
  category_nm,
  description,
  transaction_amt,
  transaction_currency_cd,
  transacted_at_utc,
  date(transacted_at_utc at time zone 'Europe/Moscow') as transaction_dt,
  total_rewards_amt,
  current_timestamp at time zone 'UTC' as __ingested_at
from
  {{ ref('core_load_t_bank_transactions') }}
where
  status_nm = 'OK'

{% if is_incremental() %}
  and __ingested_at >= (
    select
      coalesce(max(__ingested_at), '1900-01-02'::timestamp) - interval '1 day'
    from
      {{ this }}
  )
{% endif %}
