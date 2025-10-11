{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='transaction_bk'
) }}

select
  md5(
    concat_ws(
      '|',
      'bakai',
      transacted_at_utc,
      coalesce(doc_no, 'no_doc_no')
    )
  ) as transaction_bk,
  doc_no,
  transacted_at_utc,
  correspondent_account,
  debit_amt,
  credit_amt,
  description,
  exchange_rate,
  current_timestamp at time zone 'UTC' as processed_at
from
  {{ ref('stg_load_bakai_transactions') }}

{% if is_incremental() %}
  where processed_at >= (
    select
      coalesce(max(processed_at), '1900-01-02'::timestamp) - interval '1 day'
    from
      {{ this }}
  )
{% endif %}
