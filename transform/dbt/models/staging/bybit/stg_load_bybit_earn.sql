-- depends_on: {{ ref('transactions_column_map') }}
{{ config(
    materialized='incremental',
    incremental_strategy='append'
) }}

select
  {{ get_stg_columns_list_map('bybit_earn') }}
from
  {{ source('bybit', 'earn') }} as raw_earn

{% if is_incremental() %}
where not exists (
  select
    __load_key
  from
    {{ this }} as stg_earn
  where
    stg_earn.__load_key = raw_earn.__load_key
)
{% endif %}
