{{ config(
    materialized='view',
    alias='view_assets'
) }}

-- Binance assets
select
  'Asset' as type,
  'Binance' as platform_name,
  'Crypto' as category,
  null as description,
  total as amount_currency,
  asset as currency,
  null::decimal(18,2) as amount_rub,
  amount_usd,
  null::decimal(18,6) as executed_rate_rub,
  cast(price_usd as decimal(18,6)) as executed_rate_usd,
  null::decimal(18,6) as close_rate_rub,
  null::decimal(18,6) as close_rate_usd,
  null::decimal(10,2) as apy_pct,
  'finance-analytics-pipeline' as source,
  null as comments
from
  {{ ref('mart_load_binance_assets') }}

union all

-- Telegram assets
select
  'Asset' as type,
  'Telegram Wallet' as platform_name,
  'Crypto' as category,
  null as description,
  balance as amount_currency,
  asset_short_name as currency,
  null::decimal(18,2) as amount_rub,
  value_usd as amount_usd,
  null::decimal(18,6) as executed_rate_rub,
  cast(price_usd as decimal(18,6)) as executed_rate_usd,
  null::decimal(18,6) as close_rate_rub,
  null::decimal(18,6) as close_rate_usd,
  null::decimal(10,2) as apy_pct,
  'finance-analytics-pipeline' as source,
  null as comments
from
  {{ ref('mart_load_telegram_assets') }}

union all

-- Bybit assets
select
  'Asset' as type,
  'Bybit' as platform_name,
  'Crypto' as category,
  null as description,
  equity as amount_currency,
  coin as currency,
  null::decimal(18,2) as amount_rub,
  usd_value as amount_usd,
  null::decimal(18,6) as executed_rate_rub,
  cast(usd_value / equity as decimal(18,6)) as executed_rate_usd,
  null::decimal(18,6) as close_rate_rub,
  null::decimal(18,6) as close_rate_usd,
  null::decimal(10,2) as apy_pct,
  'finance-analytics-pipeline' as source,
  null as comments
from
  {{ ref('mart_load_bybit_assets') }}

union all

-- Telegram Earn (Staking/APY accounts)
select
  'Asset' as type,
  'Telegram Wallet' as platform_name,
  'Crypto APY' as category,
  null as description,
  wallet_balance as amount_currency,
  asset as currency,
  null::decimal(18,2) as amount_rub,
  coalesce(amount_usd, 0) + coalesce(total_yield_usd, 0) as amount_usd,
  null::decimal(18,6) as executed_rate_rub,
  cast(price_usd as decimal(18,6)) as executed_rate_usd,
  null::decimal(18,6) as close_rate_rub,
  null::decimal(18,6) as close_rate_usd,
  cast(apy_pct as decimal(10,2)) as apy_pct,
  'finance-analytics-pipeline' as source,
  'Yield earned: $' || cast(total_yield_usd as varchar) as comments
from
  {{ ref('mart_load_telegram_earn') }}

union all

-- Bybit Earn (Staking/APY accounts)
select
  'Asset' as type,
  'Bybit' as platform_name,
  'Crypto APY' as category,
  'Bybit - ' || asset || ' Earn' as description,
  wallet_balance as amount_currency,
  asset as currency,
  null::decimal(18,2) as amount_rub,
  coalesce(amount_usd, 0) + coalesce(total_yield_usd, 0) as amount_usd,
  null::decimal(18,6) as executed_rate_rub,
  cast(price_usd as decimal(18,6)) as executed_rate_usd,
  null::decimal(18,6) as close_rate_rub,
  null::decimal(18,6) as close_rate_usd,
  cast(apy as decimal(10,2)) as apy_pct,
  'finance-analytics-pipeline' as source,
  'Yield earned: $' || cast(total_yield_usd as varchar) as comments
from
  {{ ref('mart_load_bybit_earn') }}
