{{ config(materialized='table') }}

-- Current versions only from SCD2 snapshot
-- Joins with aggregated yield data to calculate total_yield_usd per asset
select
    snap.earn_bk,
    snap.asset,
    snap.apy,
    snap.wallet_balance,
    snap.price_usd,
    case
        when snap.asset in ('USDT', 'USDC', 'USD') then snap.wallet_balance
        when snap.price_usd is not null then snap.wallet_balance * snap.price_usd
        else null
    end as amount_usd,
    coalesce(yield_agg.total_yield_usd, 0) as total_yield_usd,
    snap.processed_at
from {{ ref('snap_bybit_earn') }} as snap
left join (
    select
        coin,
        sum(
            case
                when coin in ('USDT', 'USDC', 'USD') then amount
                when price_usd is not null then amount * price_usd
                else 0
            end
        ) as total_yield_usd
    from {{ ref('core_load_bybit_earn_yield') }}
    group by coin
) as yield_agg on snap.asset = yield_agg.coin
where snap.dbt_valid_to is null
