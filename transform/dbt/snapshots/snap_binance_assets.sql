{% snapshot snap_binance_assets %}
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
    md5(concat_ws('|', 'binance_assets', asset)) as asset_bk,
    asset,
    free,
    locked,
    total,
    amount_usd,
    price_usd,
    processed_at
from {{ ref('stg_load_binance_assets') }}

{% endsnapshot %}
