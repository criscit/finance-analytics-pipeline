{{ config(materialized='table') }}

-- Current versions only from SCD2 snapshot
select
    earn_bk,
    asset,
    apy_pct,
    wallet_balance,
    price_usd,
    case
        when asset in ('USDT', 'USDC', 'USD') then wallet_balance
        when price_usd is not null then wallet_balance * price_usd
        else null
    end as amount_usd,
    yield_usd as total_yield_usd,
    processed_at
from {{ ref('snap_telegram_earn') }}
where dbt_valid_to is null
