{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='transaction_bk'
) }}

select
  md5(
    concat_ws(
      '|',
      't_bank',
      transacted_at_utc,
      transaction_amt,
      coalesce(card_last4, 'no_card_last4')
    )
  ) as transaction_bk,
  card_last4,
  status_nm,
  category_nm,
  description,
  transaction_amt,
  transaction_currency_cd,
  transacted_at_utc,
  total_rewards_amt,
  current_timestamp at time zone 'UTC' AS __ingested_at
from
  {{ ref('stg_load_t_bank_transactions') }}

{% if is_incremental() %}
  where __ingested_at >= (
    select
      coalesce(max(__ingested_at), '1900-01-02'::timestamp) - interval '1 day'
    from
      {{ this }}
  )
{% endif %}
