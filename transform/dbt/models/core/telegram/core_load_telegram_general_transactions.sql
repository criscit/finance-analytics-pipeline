{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='transaction_bk'
) }}

select
  md5(
    concat_ws(
      '|',
      'telegram_general_transactions',
      coalesce(cast(amount_in as varchar), '<null>'),
      coalesce(currency_in, '<null>'),
      coalesce(cast(amount_out as varchar), '<null>'),
      coalesce(currency_out, '<null>'),
      transaction_date,
      transaction_time
    )
  ) as transaction_bk,
  transaction_type,
  status,
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
  source,
  raw_payload,
  -- Combine date and time into timestamp
  case
    when transaction_date is not null and nullif(transaction_time, '') is not null
    then transaction_date + cast(transaction_time as time)
    else null
  end as transacted_at,
  transaction_date,
  transaction_time,
  parsed_at,
  current_timestamp at time zone 'UTC' as processed_at
from
  {{ ref('stg_load_telegram_general_transactions') }}

{% if is_incremental() %}
  where processed_at >= (
    select
      coalesce(max(processed_at), '1900-01-02'::timestamp) - interval '1 day'
    from
      {{ this }}
  )
{% endif %}
