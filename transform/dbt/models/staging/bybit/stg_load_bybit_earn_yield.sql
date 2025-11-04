-- depends_on: {{ ref('transactions_column_map') }}
{{ config(
    materialized='incremental',
    incremental_strategy='append'
) }}

select
  {{ get_stg_columns_list_map('bybit_earn_yield') }}
from
  {{ source('bybit', 'earn_yield') }} as raw_earn_yield

{% if is_incremental() %}
where not exists (
  select
    __load_key
  from
    {{ this }} as stg_earn_yield
  where
    stg_earn_yield.__load_key = raw_earn_yield.__load_key
)
{% endif %}
