-- depends_on: {{ ref('transactions_column_map') }}
{{ config(
    materialized='incremental',
    incremental_strategy='append'
) }}

select
  {{ get_stg_columns_list_map('bybit_internal_transfers') }}
from
  {{ source('bybit', 'internal_transfers') }} as raw_internal_transfers

{% if is_incremental() %}
where not exists (
  select
    __load_key
  from
    {{ this }} as stg_internal_transfers
  where
    stg_internal_transfers.__load_key = raw_internal_transfers.__load_key
)
{% endif %}
