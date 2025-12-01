{{ config(
    materialized='incremental',
    unique_key='transaction_bk'
) }}

with source_data as (
  select
    transaction_bk,
    description,
    -1.0 * debit_amt as transaction_amt,
    exchange_rate,
    transacted_at_utc,
    date(transacted_at_utc at time zone 'Europe/Moscow') as transaction_dt,
    current_timestamp at time zone 'UTC' as processed_at
  from
    {{ ref('core_load_bakai_transactions') }}
  where
    debit_amt > 0

  union all

  select
    transaction_bk,
    description,
    credit_amt as transaction_amt,
    exchange_rate,
    transacted_at_utc,
    date(transacted_at_utc at time zone 'Europe/Moscow') as transaction_dt,
    current_timestamp at time zone 'UTC' as processed_at
  from
    {{ ref('core_load_bakai_transactions') }}
  where
    credit_amt > 0
)

select * from source_data
{% if is_incremental() %}
where processed_at >= (
  select
    coalesce(max(processed_at), '1900-01-02'::timestamp) - interval '1 day'
  from
    {{ this }}
)
and transaction_bk not in (
  select transaction_bk from {{ this }}
)
{% endif %}
