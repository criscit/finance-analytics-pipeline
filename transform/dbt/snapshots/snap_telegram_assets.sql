{% snapshot snap_telegram_assets %}
{{
    config(
        target_schema=generate_schema_name('core'),
        unique_key='asset_bk',
        strategy='timestamp',
        updated_at='processed_at',
        invalidate_hard_deletes=True
    )
}}

select
    md5(concat_ws('|', 'telegram_assets', asset_full_name)) as asset_bk,
    asset_full_name,
    asset_short_name,
    price_usd,
    change_24h_pct,
    balance,
    value_usd,
    processed_at
from {{ ref('stg_load_telegram_assets') }}

{% endsnapshot %}
