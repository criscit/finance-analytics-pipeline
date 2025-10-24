-- depends_on: {{ ref('transactions_column_map') }}
{{ config(
    materialized='incremental',
    incremental_strategy='append'
) }}

select
  {{ get_stg_columns_list_map('binance_fiat_payments') }}
from
  {{ source('binance', 'fiat_payments') }} as raw_fiat_payments

{% if is_incremental() %}
where not exists (
  select
    __load_key
  from
    {{ this }} as stg_fiat_payments
  where
    stg_fiat_payments.__load_key = raw_fiat_payments.__load_key
)
{% endif %}
