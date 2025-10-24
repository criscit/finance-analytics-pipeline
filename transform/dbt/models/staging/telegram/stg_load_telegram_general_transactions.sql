-- depends_on: {{ ref('transactions_column_map') }}
{{ config(
    materialized='incremental',
    incremental_strategy='append'
) }}

select
  {{ get_stg_columns_list_map('telegram_general_transactions') }}
from
  {{ source('telegram', 'general_transactions') }} as raw_general_transactions

{% if is_incremental() %}
where not exists (
  select
    __load_key
  from
    {{ this }} as stg_general_transactions
  where
    stg_general_transactions.__load_key = raw_general_transactions.__load_key
)
{% endif %}
