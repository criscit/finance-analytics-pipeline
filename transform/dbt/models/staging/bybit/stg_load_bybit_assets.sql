-- depends_on: {{ ref('transactions_column_map') }}
{{ config(
    materialized='incremental',
    incremental_strategy='append'
) }}

select
  {{ get_stg_columns_list_map('bybit_assets') }}
from
  {{ source('bybit', 'assets') }} as raw_assets

{% if is_incremental() %}
where not exists (
  select
    __load_key
  from
    {{ this }} as stg_assets
  where
    stg_assets.__load_key = raw_assets.__load_key
)
{% endif %}
