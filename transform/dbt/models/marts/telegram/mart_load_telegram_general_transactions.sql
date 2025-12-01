{{ config(
    materialized='incremental',
    unique_key='transaction_bk'
) }}

select
  transaction_bk,
  transaction_type,
  amount_in,
  currency_in,
  currency_in_price_usd,
  amount_out,
  currency_out,
  currency_out_price_usd,
  ex_rate_value,
  ex_rate_base,
  ex_rate_quote,
  fee_amt,
  fee_currency,
  counterparty,
  transacted_at,
  date(transacted_at at time zone 'Europe/Moscow') as transacted_dt,
  current_timestamp at time zone 'UTC' as processed_at
from
  {{ ref('core_load_telegram_general_transactions') }}
where
  status = 'Success'
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
