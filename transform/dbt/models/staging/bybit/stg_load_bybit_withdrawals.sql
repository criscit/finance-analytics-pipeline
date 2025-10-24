-- depends_on: {{ ref('transactions_column_map') }}
{{ config(
    materialized='incremental',
    incremental_strategy='append'
) }}

select
  {{ get_stg_columns_list_map('bybit_withdrawals') }}
from
  {{ source('bybit', 'withdrawals') }} as raw_withdrawals

{% if is_incremental() %}
where not exists (
  select
    __load_key
  from
    {{ this }} as stg_withdrawals
  where
    stg_withdrawals.__load_key = raw_withdrawals.__load_key
)
{% endif %}
