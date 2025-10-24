-- depends_on: {{ ref('transactions_column_map') }}
{{ config(
    materialized='incremental',
    incremental_strategy='append'
) }}

select
  {{ get_stg_columns_list_map('binance_spot_orders') }}
from
  {{ source('binance', 'spot_orders') }} as raw_spot_orders

{% if is_incremental() %}
where not exists (
  select
    __load_key
  from
    {{ this }} as stg_spot_orders
  where
    stg_spot_orders.__load_key = raw_spot_orders.__load_key
)
{% endif %}
