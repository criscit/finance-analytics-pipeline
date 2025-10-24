-- depends_on: {{ ref('transactions_column_map') }}
{{ config(
    materialized='incremental',
    incremental_strategy='append'
) }}

select
  {{ get_stg_columns_list_map('bybit_deposits') }}
from
  {{ source('bybit', 'deposits') }} as raw_deposits

{% if is_incremental() %}
where not exists (
  select
    __load_key
  from
    {{ this }} as stg_deposits
  where
    stg_deposits.__load_key = raw_deposits.__load_key
)
{% endif %}
