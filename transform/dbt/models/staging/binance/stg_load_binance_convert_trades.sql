-- depends_on: {{ ref('transactions_column_map') }}
{{ config(
    materialized='incremental',
    incremental_strategy='append'
) }}

select
  {{ get_stg_columns_list_map('binance_convert_trades') }}
from
  {{ source('binance', 'convert_trades') }} as raw_convert_trades

{% if is_incremental() %}
where not exists (
  select
    __load_key
  from
    {{ this }} as stg_convert_trades
  where
    stg_convert_trades.__load_key = raw_convert_trades.__load_key
)
{% endif %}
