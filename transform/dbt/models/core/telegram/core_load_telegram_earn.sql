{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='earn_bk'
) }}

select
  md5(
    concat_ws(
      '|',
      'telegram_earn',
      asset
    )
  ) as earn_bk,
  asset,
  cast(regexp_replace(coalesce(apy_pct, '0'), '[^0-9.-]', '', 'g') as double) as apy_pct,
  cast(regexp_replace(coalesce(value_usd, '0'), '[^0-9.-]', '', 'g') as double) as value_usd,
  cast(regexp_replace(coalesce(yield_usd, '0'), '[^0-9.-]', '', 'g') as double) as yield_usd,
  current_timestamp at time zone 'UTC' as processed_at
from
  {{ ref('stg_load_telegram_earn') }}

{% if is_incremental() %}
  where processed_at >= (
    select
      coalesce(max(processed_at), '1900-01-02'::timestamp) - interval '1 day'
    from
      {{ this }}
  )
{% endif %}
