{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='asset_bk'
) }}

select
  md5(
    concat_ws(
      '|',
      'telegram_assets',
      asset_full_name
    )
  ) as asset_bk,
  asset_full_name,
  asset_short_name,
  cast(regexp_replace(coalesce(price_usd, '0'), '[^0-9.-]', '', 'g') as double) as price_usd,
  cast(regexp_replace(coalesce(change_24h_pct, '0'), '[^0-9.-]', '', 'g') as double) as change_24h_pct,
  balance,
  cast(regexp_replace(coalesce(value_usd, '0'), '[^0-9.-]', '', 'g') as double) as value_usd,
  current_timestamp at time zone 'UTC' as processed_at
from
  {{ ref('stg_load_telegram_assets') }}

{% if is_incremental() %}
  where processed_at >= (
    select
      coalesce(max(processed_at), '1900-01-02'::timestamp) - interval '1 day'
    from
      {{ this }}
  )
{% endif %}
