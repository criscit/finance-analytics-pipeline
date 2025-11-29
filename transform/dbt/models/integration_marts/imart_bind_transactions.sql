{{ config(
    materialized='view'
) }}

-- T Bank transactions (RUB)
select
  transacted_at_utc as transacted_at,
  'T Bank' as platform_name,
  category_nm as category,
  description,
  transaction_amt as amount_currency,
  transaction_currency_cd as currency,
  transaction_amt as amount_rub,  -- T Bank is in RUB
  null::decimal(18,2) as amount_usd,  -- No USD conversion available yet
  null::decimal(18,6) as executed_rate_rub,  -- No rate tracking for T Bank yet
  null::decimal(18,6) as executed_rate_usd,  -- No rate tracking for T Bank yet
  null::decimal(18,6) as close_rate_rub,  -- No rate tracking for T Bank yet
  null::decimal(18,6) as close_rate_usd  -- No rate tracking for T Bank yet
from
  {{ ref('mart_load_t_bank_transactions') }}

union all

-- T Bank cashback (RUB)
select
  transacted_at_utc as transacted_at,
  'T Bank' as platform_name,
  'Cashback' as category,
  'Cashback and other rewards' as description,
  total_rewards_amt as amount_currency,
  transaction_currency_cd as currency,
  total_rewards_amt as amount_rub,  -- T Bank is in RUB
  null::decimal(18,2) as amount_usd,
  null::decimal(18,6) as executed_rate_rub,
  null::decimal(18,6) as executed_rate_usd,
  null::decimal(18,6) as close_rate_rub,
  null::decimal(18,6) as close_rate_usd
from
  {{ ref('mart_load_t_bank_transactions') }}
where
  total_rewards_amt > 0

union all

-- BakAi Bank transactions (KGS with USD exchange rate)
select
  transacted_at_utc as transacted_at,
  'BakAi Bank' as platform_name,
  null as category,
  description,
  transaction_amt as amount_currency,
  null as currency,
  null::decimal(18,2) as amount_rub,
  null as amount_usd,
  null::decimal(18,6) as executed_rate_rub,
  exchange_rate as executed_rate_usd,
  null::decimal(18,6) as close_rate_rub,
  null::decimal(18,6) as close_rate_usd
from
  {{ ref('mart_load_bakai_transactions') }}

union all

-- Binance Spot Orders - Sold crypto (what you gave up)
select
  executed_at as transacted_at,
  'Binance' as platform_name,
  'Spot Trade' as category,
  order_side || ' ' || pair || ' @ ' || cast(average_price as varchar) || ' - Sold' as description,
  case
    when order_side = 'BUY' then -1.0 * trading_total  -- When buying, you sell quote currency (USDT)
    when order_side = 'SELL' then -1.0 * executed_amt  -- When selling, you sell base currency (BTC)
  end as amount_currency,
  case
    when order_side = 'BUY' then trading_total_currency  -- Quote currency
    when order_side = 'SELL' then executed_amt_currency  -- Base currency
  end as currency,
  null::decimal(18,2) as amount_rub,
  case
    when order_side = 'BUY' and trading_total_currency in ('USDT', 'USDC') then -1.0 * trading_total
    when order_side = 'SELL' and executed_amt_currency in ('USDT', 'USDC') then -1.0 * executed_amt
    else null
  end as amount_usd,
  null::decimal(18,6) as executed_rate_rub,
  average_price as executed_rate_usd,
  null::decimal(18,6) as close_rate_rub,
  null::decimal(18,6) as close_rate_usd
from
  {{ ref('mart_load_binance_spot_orders') }}

union all

-- Binance Spot Orders - Bought crypto (what you received)
select
  executed_at as transacted_at,
  'Binance' as platform_name,
  'Spot Trade' as category,
  order_side || ' ' || pair || ' @ ' || cast(average_price as varchar) || ' - Bought' as description,
  case
    when order_side = 'BUY' then executed_amt  -- When buying, you receive base currency (BTC)
    when order_side = 'SELL' then trading_total  -- When selling, you receive quote currency (USDT)
  end as amount_currency,
  case
    when order_side = 'BUY' then executed_amt_currency  -- Base currency
    when order_side = 'SELL' then trading_total_currency  -- Quote currency
  end as currency,
  null::decimal(18,2) as amount_rub,
  case
    when order_side = 'BUY' and executed_amt_currency in ('USDT', 'USDC') then executed_amt
    when order_side = 'SELL' and trading_total_currency in ('USDT', 'USDC') then trading_total
    else null
  end as amount_usd,
  null::decimal(18,6) as executed_rate_rub,
  average_price as executed_rate_usd,
  null::decimal(18,6) as close_rate_rub,
  null::decimal(18,6) as close_rate_usd
from
  {{ ref('mart_load_binance_spot_orders') }}

union all

-- Binance P2P Trades - Fiat side
select
  created_at as transacted_at,
  'Binance' as platform_name,
  'P2P Trade' as category,
  trade_type || ' ' || cast(amount as varchar) || ' ' || asset || ' via ' || pay_method_name || ' - Fiat' as description,
  case
    when trade_type = 'BUY' then -1.0 * total_price  -- When buying crypto, you pay fiat
    when trade_type = 'SELL' then total_price  -- When selling crypto, you receive fiat
  end as amount_currency,
  fiat as currency,
  null::decimal(18,2) as amount_rub,
  case
    when fiat = 'USD' and trade_type = 'BUY' then -1.0 * total_price
    when fiat = 'USD' and trade_type = 'SELL' then total_price
    else null
  end as amount_usd,
  null::decimal(18,6) as executed_rate_rub,
  unit_price as executed_rate_usd,
  null::decimal(18,6) as close_rate_rub,
  null::decimal(18,6) as close_rate_usd
from
  {{ ref('mart_load_binance_p2p_trades') }}

union all

-- Binance P2P Trades - Crypto side
select
  created_at as transacted_at,
  'Binance' as platform_name,
  'P2P Trade' as category,
  trade_type || ' ' || cast(amount as varchar) || ' ' || asset || ' via ' || pay_method_name || ' - Crypto' as description,
  case
    when trade_type = 'BUY' then amount  -- When buying crypto, you receive crypto
    when trade_type = 'SELL' then -1.0 * amount  -- When selling crypto, you give up crypto
  end as amount_currency,
  asset as currency,
  null::decimal(18,2) as amount_rub,
  null::decimal(18,2) as amount_usd,  -- Crypto side, not USD
  null::decimal(18,6) as executed_rate_rub,
  unit_price as executed_rate_usd,
  null::decimal(18,6) as close_rate_rub,
  asset_price_usd as close_rate_usd
from
  {{ ref('mart_load_binance_p2p_trades') }}

union all

-- Binance P2P Trades - Fee
select
  created_at as transacted_at,
  'Binance' as platform_name,
  'Fee' as category,
  'P2P trading fee for ' || trade_type || ' ' || asset as description,
  -1.0 * commission as amount_currency,
  asset as currency,
  null::decimal(18,2) as amount_rub,
  case when asset in ('USDT', 'USDC') then -1.0 * commission else null end as amount_usd,
  null::decimal(18,6) as executed_rate_rub,
  null::decimal(18,6) as executed_rate_usd,
  null::decimal(18,6) as close_rate_rub,
  asset_price_usd as close_rate_usd
from
  {{ ref('mart_load_binance_p2p_trades') }}
where
  commission > 0

union all

-- Binance Convert Trades - From (what you gave up)
select
  created_at as transacted_at,
  'Binance' as platform_name,
  'Convert' as category,
  'Convert ' || cast(from_amt as varchar) || ' ' || from_asset || ' to ' || cast(to_amt as varchar) || ' ' || to_asset || ' - From' as description,
  -1.0 * from_amt as amount_currency,
  from_asset as currency,
  null::decimal(18,2) as amount_rub,
  case
    when from_asset in ('USDT', 'USDC') then -1.0 * from_amt
    when from_price_usd is not null then -1.0 * from_amt * from_price_usd
    else null
  end as amount_usd,
  null::decimal(18,6) as executed_rate_rub,
  ratio as executed_rate_usd,
  null::decimal(18,6) as close_rate_rub,
  from_price_usd as close_rate_usd
from
  {{ ref('mart_load_binance_convert_trades') }}

union all

-- Binance Convert Trades - To (what you received)
select
  created_at as transacted_at,
  'Binance' as platform_name,
  'Convert' as category,
  'Convert ' || cast(from_amt as varchar) || ' ' || from_asset || ' to ' || cast(to_amt as varchar) || ' ' || to_asset || ' - To' as description,
  to_amt as amount_currency,
  to_asset as currency,
  null::decimal(18,2) as amount_rub,
  case
    when to_asset in ('USDT', 'USDC') then to_amt
    when to_price_usd is not null then to_amt * to_price_usd
    else null
  end as amount_usd,
  null::decimal(18,6) as executed_rate_rub,
  ratio as executed_rate_usd,
  null::decimal(18,6) as close_rate_rub,
  to_price_usd as close_rate_usd
from
  {{ ref('mart_load_binance_convert_trades') }}

union all

-- Bybit Spot Orders - Sold crypto (what you gave up)
select
  executed_at as transacted_at,
  'Bybit' as platform_name,
  'Spot Trade' as category,
  side || ' ' || symbol || ' @ ' || cast(exec_price as varchar) || ' - Sold' as description,
  case
    when side = 'Buy' then -1.0 * exec_value  -- When buying, you sell quote currency (USDT)
    when side = 'Sell' then -1.0 * exec_qty   -- When selling, you sell base crypto
  end as amount_currency,
  case
    when side = 'Buy' then 'USDT'  -- Quote currency
    when side = 'Sell' then substring(symbol, 1, length(symbol) - 4)  -- Base crypto (remove USDT suffix)
  end as currency,
  null::decimal(18,2) as amount_rub,
  case
    when side = 'Buy' then -1.0 * exec_value  -- USDT spent
    when side = 'Sell' then null  -- Selling crypto, not USD
  end as amount_usd,
  null::decimal(18,6) as executed_rate_rub,
  exec_price as executed_rate_usd,
  null::decimal(18,6) as close_rate_rub,
  null::decimal(18,6) as close_rate_usd
from
  {{ ref('mart_load_bybit_spot_orders') }}

union all

-- Bybit Spot Orders - Bought crypto (what you received)
select
  executed_at as transacted_at,
  'Bybit' as platform_name,
  'Spot Trade' as category,
  side || ' ' || symbol || ' @ ' || cast(exec_price as varchar) || ' - Bought' as description,
  case
    when side = 'Buy' then exec_qty  -- When buying, you receive base crypto
    when side = 'Sell' then exec_value  -- When selling, you receive quote currency (USDT)
  end as amount_currency,
  case
    when side = 'Buy' then substring(symbol, 1, length(symbol) - 4)  -- Base crypto
    when side = 'Sell' then 'USDT'  -- Quote currency
  end as currency,
  null::decimal(18,2) as amount_rub,
  case
    when side = 'Buy' then null  -- Buying crypto, not USD
    when side = 'Sell' then exec_value  -- USDT received
  end as amount_usd,
  null::decimal(18,6) as executed_rate_rub,
  exec_price as executed_rate_usd,
  null::decimal(18,6) as close_rate_rub,
  null::decimal(18,6) as close_rate_usd
from
  {{ ref('mart_load_bybit_spot_orders') }}

union all

-- Bybit Spot Orders - Trading Fee
select
  executed_at as transacted_at,
  'Bybit' as platform_name,
  'Fee' as category,
  'Trading fee for ' || side || ' ' || symbol as description,
  -1.0 * (exec_fee + coalesce(extra_fees, 0)) as amount_currency,
  fee_currency as currency,
  null::decimal(18,2) as amount_rub,
  case when fee_currency in ('USDT', 'USDC', 'USD') then -1.0 * (exec_fee + coalesce(extra_fees, 0)) else null end as amount_usd,
  null::decimal(18,6) as executed_rate_rub,
  null::decimal(18,6) as executed_rate_usd,
  null::decimal(18,6) as close_rate_rub,
  null::decimal(18,6) as close_rate_usd
from
  {{ ref('mart_load_bybit_spot_orders') }}
where
  (exec_fee + coalesce(extra_fees, 0)) > 0

union all

-- Bybit P2P Trades - Fiat side
select
  executed_at as transacted_at,
  'Bybit' as platform_name,
  'P2P Trade' as category,
  type || ' ' || qty || ' crypto for ' || amount || ' - Fiat' as description,
  case
    when upper(type) = 'BUY' then -1.0 * fiat_amt  -- When buying crypto, you pay fiat
    when upper(type) = 'SELL' then fiat_amt  -- When selling crypto, you receive fiat
  end as amount_currency,
  fiat_currency as currency,
  null::decimal(18,2) as amount_rub,
  case
    when upper(type) = 'BUY' then -1.0 * fiat_amt
    when upper(type) = 'SELL' then fiat_amt
  end as amount_usd,
  null::decimal(18,6) as executed_rate_rub,
  price_usd as executed_rate_usd,
  null::decimal(18,6) as close_rate_rub,
  null::decimal(18,6) as close_rate_usd
from
  {{ ref('mart_load_bybit_p2p_trades') }}

union all

-- Bybit P2P Trades - Crypto side
select
  executed_at as transacted_at,
  'Bybit' as platform_name,
  'P2P Trade' as category,
  type || ' ' || qty || ' crypto for ' || amount || ' - Crypto' as description,
  case
    when upper(type) = 'BUY' then crypto_amt  -- When buying crypto, you receive crypto
    when upper(type) = 'SELL' then -1.0 * crypto_amt  -- When selling crypto, you give up crypto
  end as amount_currency,
  crypto_currency as currency,
  null::decimal(18,2) as amount_rub,
  null::decimal(18,2) as amount_usd,
  null::decimal(18,6) as executed_rate_rub,
  price_usd as executed_rate_usd,
  null::decimal(18,6) as close_rate_rub,
  null::decimal(18,6) as close_rate_usd
from
  {{ ref('mart_load_bybit_p2p_trades') }}

union all

-- Bybit P2P Trades - Fee
select
  executed_at as transacted_at,
  'Bybit' as platform_name,
  'Fee' as category,
  'P2P trading fee' as description,
  -1.0 * fee_amount as amount_currency,
  fee_currency as currency,
  null::decimal(18,2) as amount_rub,
  -1.0 * fee_amount as amount_usd,
  null::decimal(18,6) as executed_rate_rub,
  null::decimal(18,6) as executed_rate_usd,
  null::decimal(18,6) as close_rate_rub,
  null::decimal(18,6) as close_rate_usd
from
  {{ ref('mart_load_bybit_p2p_trades') }}
where
  fee_amount > 0

union all

-- Bybit Deposits
select
  success_at as transacted_at,
  'Bybit' as platform_name,
  'Deposit' as category,
  'Deposit ' || cast(amount as varchar) || ' ' || coin || ' via ' || chain as description,
  amount as amount_currency,
  coin as currency,
  null::decimal(18,2) as amount_rub,
  case
    when coin in ('USDT', 'USDC', 'USD') then amount
    when price_usd is not null then amount * price_usd
    else null
  end as amount_usd,
  null::decimal(18,6) as executed_rate_rub,
  price_usd as executed_rate_usd,
  null::decimal(18,6) as close_rate_rub,
  null::decimal(18,6) as close_rate_usd
from
  {{ ref('mart_load_bybit_deposits') }}

union all

-- Bybit Deposits - Fee
select
  success_at as transacted_at,
  'Bybit' as platform_name,
  'Fee' as category,
  'Deposit fee for ' || coin || ' via ' || chain as description,
  -1.0 * deposit_fee as amount_currency,
  coin as currency,
  null::decimal(18,2) as amount_rub,
  case
    when coin in ('USDT', 'USDC', 'USD') then -1.0 * deposit_fee
    when price_usd is not null then -1.0 * deposit_fee * price_usd
    else null
  end as amount_usd,
  null::decimal(18,6) as executed_rate_rub,
  price_usd as executed_rate_usd,
  null::decimal(18,6) as close_rate_rub,
  null::decimal(18,6) as close_rate_usd
from
  {{ ref('mart_load_bybit_deposits') }}
where
  deposit_fee > 0

union all

-- Bybit Withdrawals
select
  executed_at as transacted_at,
  'Bybit' as platform_name,
  'Withdrawal' as category,
  'Withdraw ' || cast(amount as varchar) || ' ' || coin || ' via ' || chain as description,
  -1.0 * amount as amount_currency,  -- Negative for withdrawal
  coin as currency,
  null::decimal(18,2) as amount_rub,
  case
    when coin in ('USDT', 'USDC', 'USD') then -1.0 * amount
    when price_usd is not null then -1.0 * amount * price_usd
    else null
  end as amount_usd,
  null::decimal(18,6) as executed_rate_rub,
  price_usd as executed_rate_usd,
  null::decimal(18,6) as close_rate_rub,
  null::decimal(18,6) as close_rate_usd
from
  {{ ref('mart_load_bybit_withdrawals') }}

union all

-- Bybit Withdrawals - Fee
select
  executed_at as transacted_at,
  'Bybit' as platform_name,
  'Fee' as category,
  'Withdrawal fee for ' || coin || ' via ' || chain as description,
  -1.0 * withdraw_fee as amount_currency,
  coin as currency,
  null::decimal(18,2) as amount_rub,
  case
    when coin in ('USDT', 'USDC', 'USD') then -1.0 * withdraw_fee
    when price_usd is not null then -1.0 * withdraw_fee * price_usd
    else null
  end as amount_usd,
  null::decimal(18,6) as executed_rate_rub,
  price_usd as executed_rate_usd,
  null::decimal(18,6) as close_rate_rub,
  null::decimal(18,6) as close_rate_usd
from
  {{ ref('mart_load_bybit_withdrawals') }}
where
  withdraw_fee > 0

union all

-- Telegram General Transactions
select
  transacted_at as transacted_at,
  'Telegram' as platform_name,
  transaction_type as category,
  coalesce(counterparty, 'Telegram transaction') || ' | In: ' || coalesce(cast(amount_in as varchar) || ' ' || currency_in, 'N/A') || ' | Out: ' || coalesce(cast(amount_out as varchar) || ' ' || currency_out, 'N/A') as description,
  coalesce(amount_in, -1.0 * amount_out) as amount_currency,
  coalesce(currency_in, currency_out) as currency,
  null::decimal(18,2) as amount_rub,
  case
    when currency_in in ('USDT', 'USD') then amount_in
    when currency_out in ('USDT', 'USD') then -1.0 * amount_out
    when amount_in is not null and currency_in_price_usd is not null then amount_in * currency_in_price_usd
    when amount_out is not null and currency_out_price_usd is not null then -1.0 * amount_out * currency_out_price_usd
    else null
  end as amount_usd,
  null::decimal(18,6) as executed_rate_rub,
  ex_rate_value as executed_rate_usd,
  null::decimal(18,6) as close_rate_rub,
  coalesce(currency_in_price_usd, currency_out_price_usd) as close_rate_usd
from
  {{ ref('mart_load_telegram_general_transactions') }}

union all

-- Telegram General Transactions - Fee
select
  transacted_at as transacted_at,
  'Telegram' as platform_name,
  'Fee' as category,
  'Transaction fee' as description,
  -1.0 * fee_amt as amount_currency,
  fee_currency as currency,
  null::decimal(18,2) as amount_rub,
  case when fee_currency in ('USDT', 'USD') then -1.0 * fee_amt else null end as amount_usd,
  null::decimal(18,6) as executed_rate_rub,
  null::decimal(18,6) as executed_rate_usd,
  null::decimal(18,6) as close_rate_rub,
  null::decimal(18,6) as close_rate_usd
from
  {{ ref('mart_load_telegram_general_transactions') }}
where
  fee_amt > 0

union all

-- Telegram P2P Trades - Fiat side
select
  completed_at as transacted_at,
  'Telegram' as platform_name,
  'P2P Trade' as category,
  ad_type || ' as ' || role || ' - ' || cast(net_crypto_amt as varchar) || ' ' || crypto_currency || ' for ' || cast(fiat_amt as varchar) || ' ' || fiat_currency || ' - Fiat' as description,
  case
    when upper(ad_type) = 'BUY' then -1.0 * fiat_amt  -- When buying crypto, you pay fiat
    when upper(ad_type) in ('SELL', 'SALE') then fiat_amt  -- When selling crypto, you receive fiat
  end as amount_currency,
  fiat_currency as currency,
  null::decimal(18,2) as amount_rub,
  case
    when fiat_currency = 'USD' and upper(ad_type) = 'BUY' then -1.0 * fiat_amt
    when fiat_currency = 'USD' and upper(ad_type) in ('SELL', 'SALE') then fiat_amt
    else null
  end as amount_usd,
  null::decimal(18,6) as executed_rate_rub,
  price as executed_rate_usd,
  null::decimal(18,6) as close_rate_rub,
  null::decimal(18,6) as close_rate_usd
from
  {{ ref('mart_load_telegram_p2p_trades') }}

union all

-- Telegram P2P Trades - Crypto side
select
  completed_at as transacted_at,
  'Telegram' as platform_name,
  'P2P Trade' as category,
  ad_type || ' as ' || role || ' - ' || cast(net_crypto_amt as varchar) || ' ' || crypto_currency || ' for ' || cast(fiat_amt as varchar) || ' ' || fiat_currency || ' - Crypto' as description,
  case
    when upper(ad_type) = 'BUY' then net_crypto_amt  -- When buying crypto, you receive crypto
    when upper(ad_type) in ('SELL', 'SALE') then -1.0 * net_crypto_amt  -- When selling crypto, you give up crypto
  end as amount_currency,
  crypto_currency as currency,
  null::decimal(18,2) as amount_rub,
  null::decimal(18,2) as amount_usd,
  null::decimal(18,6) as executed_rate_rub,
  price as executed_rate_usd,
  null::decimal(18,6) as close_rate_rub,
  null::decimal(18,6) as close_rate_usd
from
  {{ ref('mart_load_telegram_p2p_trades') }}

union all

-- Telegram P2P Trades - Fee
select
  completed_at as transacted_at,
  'Telegram' as platform_name,
  'Fee' as category,
  'P2P trading fee' as description,
  -1.0 * paid_fee_crypto_amt as amount_currency,
  crypto_currency as currency,
  null::decimal(18,2) as amount_rub,
  null::decimal(18,2) as amount_usd,
  null::decimal(18,6) as executed_rate_rub,
  null::decimal(18,6) as executed_rate_usd,
  null::decimal(18,6) as close_rate_rub,
  null::decimal(18,6) as close_rate_usd
from
  {{ ref('mart_load_telegram_p2p_trades') }}
where
  paid_fee_crypto_amt > 0

union all

-- Binance Fiat Orders
select
  executed_at as transacted_at,
  'Binance' as platform_name,
  'Fiat ' || transaction_type as category,
  transaction_type || ' ' || cast(indicated_amt as varchar) || ' via ' || method as description,
  case when transaction_type = 'Deposit' then amount else -1.0 * amount end as amount_currency,
  fiat_currency as currency,
  case when fiat_currency = 'RUB' then (case when transaction_type = 'Deposit' then amount else -1.0 * amount end) else null end as amount_rub,
  case when fiat_currency = 'USD' then (case when transaction_type = 'Deposit' then amount else -1.0 * amount end) else null end as amount_usd,
  case when indicated_amt > 0 then amount / indicated_amt else null end as executed_rate_rub,
  null::decimal(18,6) as executed_rate_usd,
  null::decimal(18,6) as close_rate_rub,
  null::decimal(18,6) as close_rate_usd
from
  {{ ref('mart_load_binance_fiat_orders') }}

union all

-- Binance Fiat Orders - Fee
select
  executed_at as transacted_at,
  'Binance' as platform_name,
  'Fee' as category,
  'Fiat ' || transaction_type || ' fee via ' || method as description,
  -1.0 * total_fee as amount_currency,
  fiat_currency as currency,
  case when fiat_currency = 'RUB' then -1.0 * total_fee else null end as amount_rub,
  case when fiat_currency = 'USD' then -1.0 * total_fee else null end as amount_usd,
  null::decimal(18,6) as executed_rate_rub,
  null::decimal(18,6) as executed_rate_usd,
  null::decimal(18,6) as close_rate_rub,
  null::decimal(18,6) as close_rate_usd
from
  {{ ref('mart_load_binance_fiat_orders') }}
where
  total_fee > 0

union all

-- Bybit Convert Trades - From (what you gave up)
select
  created_at as transacted_at,
  'Bybit' as platform_name,
  'Convert' as category,
  'Convert ' || cast(from_amt as varchar) || ' ' || from_coin || ' to ' || cast(to_amt as varchar) || ' ' || to_coin || ' - From' as description,
  -1.0 * from_amt as amount_currency,
  from_coin as currency,
  null::decimal(18,2) as amount_rub,
  case
    when from_coin in ('USDT', 'USDC', 'USD') then -1.0 * from_amt
    when from_price_usd is not null then -1.0 * from_amt * from_price_usd
    else null
  end as amount_usd,
  null::decimal(18,6) as executed_rate_rub,
  exchange_rate as executed_rate_usd,
  null::decimal(18,6) as close_rate_rub,
  from_price_usd as close_rate_usd
from
  {{ ref('mart_load_bybit_convert_trades') }}

union all

-- Bybit Convert Trades - To (what you received)
select
  created_at as transacted_at,
  'Bybit' as platform_name,
  'Convert' as category,
  'Convert ' || cast(from_amt as varchar) || ' ' || from_coin || ' to ' || cast(to_amt as varchar) || ' ' || to_coin || ' - To' as description,
  to_amt as amount_currency,
  to_coin as currency,
  null::decimal(18,2) as amount_rub,
  case
    when to_coin in ('USDT', 'USDC', 'USD') then to_amt
    when to_price_usd is not null then to_amt * to_price_usd
    else null
  end as amount_usd,
  null::decimal(18,6) as executed_rate_rub,
  exchange_rate as executed_rate_usd,
  null::decimal(18,6) as close_rate_rub,
  to_price_usd as close_rate_usd
from
  {{ ref('mart_load_bybit_convert_trades') }}

union all

-- Binance Fiat Payments - Fiat side
select
  executed_at as transacted_at,
  'Binance' as platform_name,
  'Fiat Payment' as category,
  transaction_type || ' ' || cast(obtain_amt as varchar) || ' ' || crypto_currency || ' for ' || cast(source_amt as varchar) || ' ' || fiat_currency || ' via ' || payment_method || ' - Fiat' as description,
  case
    when upper(transaction_type) = 'BUY' then -1.0 * source_amt  -- When buying crypto, you pay fiat
    when upper(transaction_type) = 'SELL' then source_amt  -- When selling crypto, you receive fiat
  end as amount_currency,
  fiat_currency as currency,
  null::decimal(18,2) as amount_rub,
  case
    when fiat_currency = 'USD' and upper(transaction_type) = 'BUY' then -1.0 * source_amt
    when fiat_currency = 'USD' and upper(transaction_type) = 'SELL' then source_amt
    else null
  end as amount_usd,
  null::decimal(18,6) as executed_rate_rub,
  price as executed_rate_usd,
  null::decimal(18,6) as close_rate_rub,
  null::decimal(18,6) as close_rate_usd
from
  {{ ref('mart_load_binance_fiat_payments') }}

union all

-- Binance Fiat Payments - Crypto side
select
  executed_at as transacted_at,
  'Binance' as platform_name,
  'Fiat Payment' as category,
  transaction_type || ' ' || cast(obtain_amt as varchar) || ' ' || crypto_currency || ' for ' || cast(source_amt as varchar) || ' ' || fiat_currency || ' via ' || payment_method || ' - Crypto' as description,
  case
    when upper(transaction_type) = 'BUY' then obtain_amt  -- When buying crypto, you receive crypto
    when upper(transaction_type) = 'SELL' then -1.0 * obtain_amt  -- When selling crypto, you give up crypto
  end as amount_currency,
  crypto_currency as currency,
  null::decimal(18,2) as amount_rub,
  null::decimal(18,2) as amount_usd,
  null::decimal(18,6) as executed_rate_rub,
  price as executed_rate_usd,
  null::decimal(18,6) as close_rate_rub,
  null::decimal(18,6) as close_rate_usd
from
  {{ ref('mart_load_binance_fiat_payments') }}

union all

-- Binance Fiat Payments - Fee
select
  executed_at as transacted_at,
  'Binance' as platform_name,
  'Fee' as category,
  'Fiat payment fee via ' || payment_method as description,
  -1.0 * total_fee as amount_currency,
  fiat_currency as currency,
  null::decimal(18,2) as amount_rub,
  case when fiat_currency = 'USD' then -1.0 * total_fee else null end as amount_usd,
  null::decimal(18,6) as executed_rate_rub,
  null::decimal(18,6) as executed_rate_usd,
  null::decimal(18,6) as close_rate_rub,
  null::decimal(18,6) as close_rate_usd
from
  {{ ref('mart_load_binance_fiat_payments') }}
where
  total_fee > 0

union all

-- Bybit Earn Yield (Staking/APY yield distributions)
select
  created_at_utc as transacted_at,
  'Bybit' as platform_name,
  'Yield' as category,
  'Earn yield distribution (' || yieldType || ')' as description,
  amount as amount_currency,
  coin as currency,
  null::decimal(18,2) as amount_rub,
  case
    when coin in ('USDT', 'USDC', 'USD') then amount
    when price_usd is not null then amount * price_usd
    else null
  end as amount_usd,
  null::decimal(18,6) as executed_rate_rub,
  price_usd as executed_rate_usd,
  null::decimal(18,6) as close_rate_rub,
  null::decimal(18,6) as close_rate_usd
from
  {{ ref('mart_load_bybit_earn_yield') }}
