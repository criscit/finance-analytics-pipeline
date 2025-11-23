{% snapshot snap_bybit_assets %}
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
    md5(concat_ws('|', 'bybit_assets', coin)) as asset_bk,
    coin,
    equity,
    free,
    locked,
    usd_value,
    wallet_balance,
    processed_at
from {{ ref('stg_load_bybit_assets') }}

{% endsnapshot %}
