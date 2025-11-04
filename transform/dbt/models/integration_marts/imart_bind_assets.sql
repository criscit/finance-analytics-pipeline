{{ config(
    materialized='view',
    alias='view_assets'
) }}

-- Binance assets
select
  'Asset' as "Type",
  'Crypto' as "Category",
  null as "Description",
  total as "Amount, Currency",
  asset as "Currency",
  null::decimal(18,2) as "Amount, RUB",
  amount_usd as "Amount, USD",
  null::decimal(18,6) as "Executed Rate, RUB",
  price_usd as "Executed Rate, USD",
  null::decimal(18,6) as "Close Rate, RUB",
  null::decimal(18,6) as "Close Rate, USD",
  null::decimal(10,2) as "APY, %",
  null as "Comments"
from
  {{ ref('mart_load_binance_assets') }}

union all

-- Telegram assets
select
  'Asset' as "Type",
  'Crypto' as "Category",
  null as "Description",
  balance as "Amount, Currency",
  asset_short_name as "Currency",
  null::decimal(18,2) as "Amount, RUB",
  value_usd as "Amount, USD",
  null::decimal(18,6) as "Executed Rate, RUB",
  price_usd as "Executed Rate, USD",
  null::decimal(18,6) as "Close Rate, RUB",
  null::decimal(18,6) as "Close Rate, USD",
  null::decimal(10,2) as "APY, %",
  null as "Comments"
from
  {{ ref('mart_load_telegram_assets') }}

union all

-- Bybit assets
select
  'Asset' as "Type",
  'Crypto' as "Category",
  null as "Description",
  equity as "Amount, Currency",
  coin as "Currency",
  null::decimal(18,2) as "Amount, RUB",
  usd_value as "Amount, USD",
  null::decimal(18,6) as "Executed Rate, RUB",
  usd_value / equity as "Executed Rate, USD",
  null::decimal(18,6) as "Close Rate, RUB",
  null::decimal(18,6) as "Close Rate, USD",
  null::decimal(10,2) as "APY, %",
  null as "Comments"
from
  {{ ref('mart_load_bybit_assets') }}

union all

-- Telegram Earn (Staking/APY accounts)
select
  'Asset' as "Type",
  'Crypto APY' as "Category",
  null as "Description",
  wallet_balance_coin as "Amount, Currency",
  asset as "Currency",
  null::decimal(18,2) as "Amount, RUB",
  value_usd as "Amount, USD",
  null::decimal(18,6) as "Executed Rate, RUB",
  price_per_coin_usdt as "Executed Rate, USD",
  null::decimal(18,6) as "Close Rate, RUB",
  null::decimal(18,6) as "Close Rate, USD",
  apy_pct as "APY, %",
  'Yield earned: $' || cast(yield_usd as varchar) as "Comments"
from
  {{ ref('mart_load_telegram_earn') }}

union all

-- Bybit Earn (Staking/APY accounts)
select
  'Asset' as "Type",
  'Crypto APY' as "Category",
  'Bybit - ' || asset || ' Earn' as "Description",
  wallet_balance as "Amount, Currency",
  asset as "Currency",
  null::decimal(18,2) as "Amount, RUB",
  value_usd as "Amount, USD",
  null::decimal(18,6) as "Executed Rate, RUB",
  asset_usdt_price as "Executed Rate, USD",
  null::decimal(18,6) as "Close Rate, RUB",
  null::decimal(18,6) as "Close Rate, USD",
  apy_pct as "APY, %",
  'Yield earned: $' || cast(yield_usd as varchar) as "Comments"
from
  {{ ref('mart_load_bybit_earn') }}
