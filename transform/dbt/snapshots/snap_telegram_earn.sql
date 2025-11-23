{% snapshot snap_telegram_earn %}
{{
    config(
        target_schema=generate_schema_name('core'),
        unique_key='earn_bk',
        strategy='timestamp',
        updated_at='processed_at',
        invalidate_hard_deletes=True
    )
}}

select
    md5(concat_ws('|', 'telegram_earn', asset)) as earn_bk,
    asset,
    apy_pct,
    wallet_balance,
    price_usd,
    value_usd,
    yield_usd,
    processed_at
from {{ ref('stg_load_telegram_earn') }}

{% endsnapshot %}
