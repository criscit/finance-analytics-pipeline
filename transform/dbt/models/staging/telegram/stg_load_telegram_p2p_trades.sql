-- depends_on: {{ ref('transactions_column_map') }}
{{ config(
    materialized='incremental',
    incremental_strategy='append'
) }}

select
  {{ get_stg_columns_list_map('telegram_p2p_trades') }}
from
  {{ source('telegram', 'p2p_trades') }} as raw_p2p_trades

{% if is_incremental() %}
where not exists (
  select
    __load_key
  from
    {{ this }} as stg_p2p_trades
  where
    stg_p2p_trades.__load_key = raw_p2p_trades.__load_key
)
{% endif %}
