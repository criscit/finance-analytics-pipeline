-- depends_on: {{ ref('transactions_column_map') }}
{{ config(
    materialized='incremental',
    incremental_strategy='append',
) }}

select
  {{ get_stg_columns_list_map('bakai') }}
from 
  {{ source('bakai', 'transactions') }} as raw_transactions

{% if is_incremental() %}
where not exists (
  select
    __load_key
  from
    {{ this }} as stg_transactions
  where
    stg_transactions.__load_key = raw_transactions.__load_key
)
{% endif %}
