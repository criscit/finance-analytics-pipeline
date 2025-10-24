-- depends_on: {{ ref('transactions_column_map') }}
{{ config(
    materialized='incremental',
    incremental_strategy='append'
) }}

select
  {{ get_stg_columns_list_map('binance_fiat_orders') }}
from
  {{ source('binance', 'fiat_orders') }} as raw_fiat_orders

{% if is_incremental() %}
where not exists (
  select
    __load_key
  from
    {{ this }} as stg_fiat_orders
  where
    stg_fiat_orders.__load_key = raw_fiat_orders.__load_key
)
{% endif %}
