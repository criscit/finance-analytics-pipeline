{% snapshot snap_bybit_earn %}
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
    md5(concat_ws('|', 'bybit_earn', asset)) as earn_bk,
    asset,
    apy,
    wallet_balance,
    price_usd,
    value_usd,
    processed_at
from {{ ref('stg_load_bybit_earn') }}

{% endsnapshot %}
