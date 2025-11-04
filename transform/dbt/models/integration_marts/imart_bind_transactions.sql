{{ config(
    materialized='view',
    alias='view_transactions'
) }}

-- T Bank transactions (RUB)
select
  transaction_dt as "Date",
  transacted_at_utc as "Transaction Timestamp",
  'T Bank' as "Platform Name",
  category_nm as "Category",
  description as "Description",
  transaction_amt as "Amount, Currency",
  transaction_currency_cd as "Currency",
  transaction_amt as "Amount, RUB",  -- T Bank is in RUB
  null::decimal(18,2) as "Amount, USD",  -- No USD conversion available yet
  null::decimal(18,6) as "Executed Rate, RUB",  -- No rate tracking for T Bank yet
  null::decimal(18,6) as "Exchange Rate, USD",  -- No rate tracking for T Bank yet
  null::decimal(18,6) as "Close Rate, RUB",  -- No rate tracking for T Bank yet
  null::decimal(18,6) as "Close Rate, USD"  -- No rate tracking for T Bank yet
from
  {{ ref('mart_load_t_bank_transactions') }}

union all

-- T Bank cashback (RUB)
select
  transaction_dt as "Date",
  transacted_at_utc as "Transaction Timestamp",
  'T Bank' as "Platform Name",
  'Cashback' as "Category",
  'Cashback and other rewards' as "Description",
  total_rewards_amt as "Amount, Currency",
  transaction_currency_cd as "Currency",
  total_rewards_amt as "Amount, RUB",  -- T Bank is in RUB
  null::decimal(18,2) as "Amount, USD",
  null::decimal(18,6) as "Executed Rate, RUB",
  null::decimal(18,6) as "Exchange Rate, USD",
  null::decimal(18,6) as "Close Rate, RUB",
  null::decimal(18,6) as "Close Rate, USD"
from
  {{ ref('mart_load_t_bank_transactions') }}
where
  total_rewards_amt > 0

union all

-- BakAi Bank transactions (KGS with USD exchange rate)
select
  transaction_dt as "Date",
  transacted_at_utc as "Transaction Timestamp",
  'BakAi Bank' as "Platform Name",
  null as "Category",
  description as "Description",
  transaction_amt as "Amount, Currency",
  null as "Currency",
  null::decimal(18,2) as "Amount, RUB",
  null as "Amount, USD",
  null::decimal(18,6) as "Executed Rate, RUB",
  exchange_rate as "Exchange Rate, USD",
  null::decimal(18,6) as "Close Rate, RUB",
  null::decimal(18,6) as "Close Rate, USD"
from
  {{ ref('mart_load_bakai_transactions') }}

union all

-- Binance Spot Orders - Sold crypto (what you gave up)
select
  date(executed_at) as "Date",
  executed_at as "Transaction Timestamp",
  'Binance' as "Platform Name",
  'Spot Trade' as "Category",
  order_side || ' ' || pair || ' @ ' || cast(average_price as varchar) || ' - Sold' as "Description",
  case
    when order_side = 'BUY' then -1.0 * trading_total  -- When buying, you sell quote currency (USDT)
    when order_side = 'SELL' then -1.0 * executed_amt  -- When selling, you sell base currency (BTC)
  end as "Amount, Currency",
  case
    when order_side = 'BUY' then trading_total_currency  -- Quote currency
    when order_side = 'SELL' then executed_amt_currency  -- Base currency
  end as "Currency",
  null::decimal(18,2) as "Amount, RUB",
  case
    when order_side = 'BUY' and trading_total_currency in ('USDT', 'USDC') then -1.0 * trading_total
    when order_side = 'SELL' and executed_amt_currency in ('USDT', 'USDC') then -1.0 * executed_amt
    else null
  end as "Amount, USD",
  null::decimal(18,6) as "Executed Rate, RUB",
  average_price as "Exchange Rate, USD",
  null::decimal(18,6) as "Close Rate, RUB",
  null::decimal(18,6) as "Close Rate, USD"
from
  {{ ref('mart_load_binance_spot_orders') }}

union all

-- Binance Spot Orders - Bought crypto (what you received)
select
  date(executed_at) as "Date",
  executed_at as "Transaction Timestamp",
  'Binance' as "Platform Name",
  'Spot Trade' as "Category",
  order_side || ' ' || pair || ' @ ' || cast(average_price as varchar) || ' - Bought' as "Description",
  case
    when order_side = 'BUY' then executed_amt  -- When buying, you receive base currency (BTC)
    when order_side = 'SELL' then trading_total  -- When selling, you receive quote currency (USDT)
  end as "Amount, Currency",
  case
    when order_side = 'BUY' then executed_amt_currency  -- Base currency
    when order_side = 'SELL' then trading_total_currency  -- Quote currency
  end as "Currency",
  null::decimal(18,2) as "Amount, RUB",
  case
    when order_side = 'BUY' and executed_amt_currency in ('USDT', 'USDC') then executed_amt
    when order_side = 'SELL' and trading_total_currency in ('USDT', 'USDC') then trading_total
    else null
  end as "Amount, USD",
  null::decimal(18,6) as "Executed Rate, RUB",
  average_price as "Exchange Rate, USD",
  null::decimal(18,6) as "Close Rate, RUB",
  null::decimal(18,6) as "Close Rate, USD"
from
  {{ ref('mart_load_binance_spot_orders') }}

union all

-- Binance P2P Trades - Fiat side
select
  date(created_at) as "Date",
  created_at as "Transaction Timestamp",
  'Binance' as "Platform Name",
  'P2P Trade' as "Category",
  trade_type || ' ' || cast(amount as varchar) || ' ' || asset || ' via ' || pay_method_name || ' - Fiat' as "Description",
  case
    when trade_type = 'BUY' then -1.0 * total_price  -- When buying crypto, you pay fiat
    when trade_type = 'SELL' then total_price  -- When selling crypto, you receive fiat
  end as "Amount, Currency",
  fiat as "Currency",
  null::decimal(18,2) as "Amount, RUB",
  case
    when fiat = 'USD' and trade_type = 'BUY' then -1.0 * total_price
    when fiat = 'USD' and trade_type = 'SELL' then total_price
    else null
  end as "Amount, USD",
  null::decimal(18,6) as "Executed Rate, RUB",
  unit_price as "Exchange Rate, USD",
  null::decimal(18,6) as "Close Rate, RUB",
  null::decimal(18,6) as "Close Rate, USD"
from
  {{ ref('mart_load_binance_p2p_trades') }}

union all

-- Binance P2P Trades - Crypto side
select
  date(created_at) as "Date",
  created_at as "Transaction Timestamp",
  'Binance' as "Platform Name",
  'P2P Trade' as "Category",
  trade_type || ' ' || cast(amount as varchar) || ' ' || asset || ' via ' || pay_method_name || ' - Crypto' as "Description",
  case
    when trade_type = 'BUY' then amount  -- When buying crypto, you receive crypto
    when trade_type = 'SELL' then -1.0 * amount  -- When selling crypto, you give up crypto
  end as "Amount, Currency",
  asset as "Currency",
  null::decimal(18,2) as "Amount, RUB",
  null::decimal(18,2) as "Amount, USD",  -- Crypto side, not USD
  null::decimal(18,6) as "Executed Rate, RUB",
  unit_price as "Exchange Rate, USD",
  null::decimal(18,6) as "Close Rate, RUB",
  null::decimal(18,6) as "Close Rate, USD"
from
  {{ ref('mart_load_binance_p2p_trades') }}

union all

-- Binance Convert Trades - From (what you gave up)
select
  date(created_at) as "Date",
  created_at as "Transaction Timestamp",
  'Binance' as "Platform Name",
  'Convert' as "Category",
  'Convert ' || cast(from_amt as varchar) || ' ' || from_asset || ' to ' || cast(to_amt as varchar) || ' ' || to_asset || ' - From' as "Description",
  -1.0 * from_amt as "Amount, Currency",
  from_asset as "Currency",
  null::decimal(18,2) as "Amount, RUB",
  case when from_asset in ('USDT', 'USDC') then -1.0 * from_amt else null end as "Amount, USD",
  null::decimal(18,6) as "Executed Rate, RUB",
  ratio as "Exchange Rate, USD",
  null::decimal(18,6) as "Close Rate, RUB",
  null::decimal(18,6) as "Close Rate, USD"
from
  {{ ref('mart_load_binance_convert_trades') }}

union all

-- Binance Convert Trades - To (what you received)
select
  date(created_at) as "Date",
  created_at as "Transaction Timestamp",
  'Binance' as "Platform Name",
  'Convert' as "Category",
  'Convert ' || cast(from_amt as varchar) || ' ' || from_asset || ' to ' || cast(to_amt as varchar) || ' ' || to_asset || ' - To' as "Description",
  to_amt as "Amount, Currency",
  to_asset as "Currency",
  null::decimal(18,2) as "Amount, RUB",
  case when to_asset in ('USDT', 'USDC') then to_amt else null end as "Amount, USD",
  null::decimal(18,6) as "Executed Rate, RUB",
  ratio as "Exchange Rate, USD",
  null::decimal(18,6) as "Close Rate, RUB",
  null::decimal(18,6) as "Close Rate, USD"
from
  {{ ref('mart_load_binance_convert_trades') }}

union all

-- Bybit Spot Orders - Sold crypto (what you gave up)
select
  date(executed_at) as "Date",
  executed_at as "Transaction Timestamp",
  'Bybit' as "Platform Name",
  'Spot Trade' as "Category",
  side || ' ' || symbol || ' @ ' || cast(exec_price as varchar) || ' - Sold' as "Description",
  case
    when side = 'Buy' then -1.0 * exec_value  -- When buying, you sell quote currency (USDT)
    when side = 'Sell' then -1.0 * exec_qty   -- When selling, you sell base crypto
  end as "Amount, Currency",
  case
    when side = 'Buy' then 'USDT'  -- Quote currency
    when side = 'Sell' then substring(symbol, 1, length(symbol) - 4)  -- Base crypto (remove USDT suffix)
  end as "Currency",
  null::decimal(18,2) as "Amount, RUB",
  case
    when side = 'Buy' then -1.0 * exec_value  -- USDT spent
    when side = 'Sell' then null  -- Selling crypto, not USD
  end as "Amount, USD",
  null::decimal(18,6) as "Executed Rate, RUB",
  exec_price as "Exchange Rate, USD",
  null::decimal(18,6) as "Close Rate, RUB",
  null::decimal(18,6) as "Close Rate, USD"
from
  {{ ref('mart_load_bybit_spot_orders') }}

union all

-- Bybit Spot Orders - Bought crypto (what you received)
select
  date(executed_at) as "Date",
  executed_at as "Transaction Timestamp",
  'Bybit' as "Platform Name",
  'Spot Trade' as "Category",
  side || ' ' || symbol || ' @ ' || cast(exec_price as varchar) || ' - Bought' as "Description",
  case
    when side = 'Buy' then exec_qty  -- When buying, you receive base crypto
    when side = 'Sell' then exec_value  -- When selling, you receive quote currency (USDT)
  end as "Amount, Currency",
  case
    when side = 'Buy' then substring(symbol, 1, length(symbol) - 4)  -- Base crypto
    when side = 'Sell' then 'USDT'  -- Quote currency
  end as "Currency",
  null::decimal(18,2) as "Amount, RUB",
  case
    when side = 'Buy' then null  -- Buying crypto, not USD
    when side = 'Sell' then exec_value  -- USDT received
  end as "Amount, USD",
  null::decimal(18,6) as "Executed Rate, RUB",
  exec_price as "Exchange Rate, USD",
  null::decimal(18,6) as "Close Rate, RUB",
  null::decimal(18,6) as "Close Rate, USD"
from
  {{ ref('mart_load_bybit_spot_orders') }}

union all

-- Bybit Spot Orders - Trading Fee
select
  date(executed_at) as "Date",
  executed_at as "Transaction Timestamp",
  'Bybit' as "Platform Name",
  'Fee' as "Category",
  'Trading fee for ' || side || ' ' || symbol as "Description",
  -1.0 * exec_fee as "Amount, Currency",
  fee_currency as "Currency",
  null::decimal(18,2) as "Amount, RUB",
  case when fee_currency in ('USDT', 'USDC', 'USD') then -1.0 * exec_fee else null end as "Amount, USD",
  null::decimal(18,6) as "Executed Rate, RUB",
  null::decimal(18,6) as "Exchange Rate, USD",
  null::decimal(18,6) as "Close Rate, RUB",
  null::decimal(18,6) as "Close Rate, USD"
from
  {{ ref('mart_load_bybit_spot_orders') }}
where
  exec_fee > 0

union all

-- Bybit P2P Trades - Fiat side
select
  date(executed_at) as "Date",
  'Bybit' as "Platform Name",
  'P2P Trade' as "Category",
  type || ' ' || cast(qty as varchar) || ' crypto for ' || cast(amount as varchar) || ' - Fiat' as "Description",
  case
    when type = 'BUY' then -1.0 * amount  -- When buying crypto, you pay fiat
    when type = 'SELL' then amount  -- When selling crypto, you receive fiat
  end as "Amount, Currency",
  'USD' as "Currency",
  null::decimal(18,2) as "Amount, RUB",
  case
    when type = 'BUY' then -1.0 * amount
    when type = 'SELL' then amount
  end as "Amount, USD",
  null::decimal(18,6) as "Executed Rate, RUB",
  price as "Exchange Rate, USD",
  null::decimal(18,6) as "Close Rate, RUB",
  null::decimal(18,6) as "Close Rate, USD"
from
  {{ ref('mart_load_bybit_p2p_trades') }}

union all

-- Bybit P2P Trades - Crypto side
select
  date(executed_at) as "Date",
  'Bybit' as "Platform Name",
  'P2P Trade' as "Category",
  type || ' ' || cast(qty as varchar) || ' crypto for ' || cast(amount as varchar) || ' - Crypto' as "Description",
  case
    when type = 'BUY' then qty  -- When buying crypto, you receive crypto
    when type = 'SELL' then -1.0 * qty  -- When selling crypto, you give up crypto
  end as "Amount, Currency",
  'CRYPTO' as "Currency",  -- Need to determine actual crypto currency from data
  null::decimal(18,2) as "Amount, RUB",
  null::decimal(18,2) as "Amount, USD",
  null::decimal(18,6) as "Executed Rate, RUB",
  price as "Exchange Rate, USD",
  null::decimal(18,6) as "Close Rate, RUB",
  null::decimal(18,6) as "Close Rate, USD"
from
  {{ ref('mart_load_bybit_p2p_trades') }}

union all

-- Bybit P2P Trades - Fee
select
  date(executed_at) as "Date",
  'Bybit' as "Platform Name",
  'Fee' as "Category",
  'P2P trading fee' as "Description",
  -1.0 * fee as "Amount, Currency",
  'USD' as "Currency",
  null::decimal(18,2) as "Amount, RUB",
  -1.0 * fee as "Amount, USD",
  null::decimal(18,6) as "Executed Rate, RUB",
  null::decimal(18,6) as "Exchange Rate, USD",
  null::decimal(18,6) as "Close Rate, RUB",
  null::decimal(18,6) as "Close Rate, USD"
from
  {{ ref('mart_load_bybit_p2p_trades') }}
where
  fee > 0

union all

-- Bybit Deposits
select
  date(success_at) as "Date",
  'Bybit' as "Platform Name",
  'Deposit' as "Category",
  'Deposit ' || cast(amount as varchar) || ' ' || coin || ' via ' || chain as "Description",
  amount as "Amount, Currency",
  coin as "Currency",
  null::decimal(18,2) as "Amount, RUB",
  case when coin in ('USDT', 'USDC', 'USD') then amount else null end as "Amount, USD",
  null::decimal(18,6) as "Executed Rate, RUB",
  null::decimal(18,6) as "Exchange Rate, USD",
  null::decimal(18,6) as "Close Rate, RUB",
  null::decimal(18,6) as "Close Rate, USD"
from
  {{ ref('mart_load_bybit_deposits') }}

union all

-- Bybit Deposits - Fee
select
  date(success_at) as "Date",
  'Bybit' as "Platform Name",
  'Fee' as "Category",
  'Deposit fee for ' || coin || ' via ' || chain as "Description",
  -1.0 * deposit_fee as "Amount, Currency",
  coin as "Currency",
  null::decimal(18,2) as "Amount, RUB",
  case when coin in ('USDT', 'USDC', 'USD') then -1.0 * deposit_fee else null end as "Amount, USD",
  null::decimal(18,6) as "Executed Rate, RUB",
  null::decimal(18,6) as "Exchange Rate, USD",
  null::decimal(18,6) as "Close Rate, RUB",
  null::decimal(18,6) as "Close Rate, USD"
from
  {{ ref('mart_load_bybit_deposits') }}
where
  deposit_fee > 0

union all

-- Bybit Withdrawals
select
  date(updated_at) as "Date",
  'Bybit' as "Platform Name",
  'Withdrawal' as "Category",
  'Withdraw ' || cast(amount as varchar) || ' ' || coin || ' via ' || chain as "Description",
  -1.0 * amount as "Amount, Currency",  -- Negative for withdrawal
  coin as "Currency",
  null::decimal(18,2) as "Amount, RUB",
  case when coin in ('USDT', 'USDC', 'USD') then -1.0 * amount else null end as "Amount, USD",
  null::decimal(18,6) as "Executed Rate, RUB",
  null::decimal(18,6) as "Exchange Rate, USD",
  null::decimal(18,6) as "Close Rate, RUB",
  null::decimal(18,6) as "Close Rate, USD"
from
  {{ ref('mart_load_bybit_withdrawals') }}

union all

-- Bybit Withdrawals - Fee
select
  date(updated_at) as "Date",
  'Bybit' as "Platform Name",
  'Fee' as "Category",
  'Withdrawal fee for ' || coin || ' via ' || chain as "Description",
  -1.0 * withdraw_fee as "Amount, Currency",
  coin as "Currency",
  null::decimal(18,2) as "Amount, RUB",
  case when coin in ('USDT', 'USDC', 'USD') then -1.0 * withdraw_fee else null end as "Amount, USD",
  null::decimal(18,6) as "Executed Rate, RUB",
  null::decimal(18,6) as "Exchange Rate, USD",
  null::decimal(18,6) as "Close Rate, RUB",
  null::decimal(18,6) as "Close Rate, USD"
from
  {{ ref('mart_load_bybit_withdrawals') }}
where
  withdraw_fee > 0

union all

-- Telegram General Transactions
select
  date(coalesce(transacted_at, transaction_date)) as "Date",
  'Telegram Wallet' as "Platform Name",
  transaction_type as "Category",
  coalesce(counterparty, 'Telegram transaction') || ' | In: ' || coalesce(cast(amount_in as varchar) || ' ' || currency_in, 'N/A') || ' | Out: ' || coalesce(cast(amount_out as varchar) || ' ' || currency_out, 'N/A') as "Description",
  coalesce(amount_in, -1.0 * amount_out) as "Amount, Currency",
  coalesce(currency_in, currency_out) as "Currency",
  null::decimal(18,2) as "Amount, RUB",
  case
    when currency_in in ('USDT', 'USD') then amount_in
    when currency_out in ('USDT', 'USD') then -1.0 * amount_out
    else null
  end as "Amount, USD",
  null::decimal(18,6) as "Executed Rate, RUB",
  ex_rate_value as "Exchange Rate, USD",
  null::decimal(18,6) as "Close Rate, RUB",
  null::decimal(18,6) as "Close Rate, USD"
from
  {{ ref('mart_load_telegram_general_transactions') }}
where
  status = 'completed'

union all

-- Telegram General Transactions - Fee
select
  date(coalesce(transacted_at, transaction_date)) as "Date",
  'Telegram Wallet' as "Platform Name",
  'Fee' as "Category",
  'Transaction fee' as "Description",
  -1.0 * fee_amt as "Amount, Currency",
  fee_currency as "Currency",
  null::decimal(18,2) as "Amount, RUB",
  case when fee_currency in ('USDT', 'USD') then -1.0 * fee_amt else null end as "Amount, USD",
  null::decimal(18,6) as "Executed Rate, RUB",
  null::decimal(18,6) as "Exchange Rate, USD",
  null::decimal(18,6) as "Close Rate, RUB",
  null::decimal(18,6) as "Close Rate, USD"
from
  {{ ref('mart_load_telegram_general_transactions') }}
where
  status = 'completed'
  and fee_amt > 0

union all

-- Telegram P2P Trades - Fiat side
select
  date(completed_at) as "Date",
  'Telegram Wallet' as "Platform Name",
  'P2P Trade' as "Category",
  ad_type || ' as ' || role || ' - ' || cast(net_crypto_amt as varchar) || ' ' || crypto_currency || ' for ' || cast(fiat_amt as varchar) || ' ' || fiat_currency || ' - Fiat' as "Description",
  case
    when ad_type = 'BUY' then -1.0 * fiat_amt  -- When buying crypto, you pay fiat
    when ad_type = 'SELL' then fiat_amt  -- When selling crypto, you receive fiat
  end as "Amount, Currency",
  fiat_currency as "Currency",
  null::decimal(18,2) as "Amount, RUB",
  case
    when fiat_currency = 'USD' and ad_type = 'BUY' then -1.0 * fiat_amt
    when fiat_currency = 'USD' and ad_type = 'SELL' then fiat_amt
    else null
  end as "Amount, USD",
  null::decimal(18,6) as "Executed Rate, RUB",
  price as "Exchange Rate, USD",
  null::decimal(18,6) as "Close Rate, RUB",
  null::decimal(18,6) as "Close Rate, USD"
from
  {{ ref('mart_load_telegram_p2p_trades') }}

union all

-- Telegram P2P Trades - Crypto side
select
  date(completed_at) as "Date",
  'Telegram Wallet' as "Platform Name",
  'P2P Trade' as "Category",
  ad_type || ' as ' || role || ' - ' || cast(net_crypto_amt as varchar) || ' ' || crypto_currency || ' for ' || cast(fiat_amt as varchar) || ' ' || fiat_currency || ' - Crypto' as "Description",
  case
    when ad_type = 'BUY' then net_crypto_amt  -- When buying crypto, you receive crypto
    when ad_type = 'SELL' then -1.0 * net_crypto_amt  -- When selling crypto, you give up crypto
  end as "Amount, Currency",
  crypto_currency as "Currency",
  null::decimal(18,2) as "Amount, RUB",
  null::decimal(18,2) as "Amount, USD",
  null::decimal(18,6) as "Executed Rate, RUB",
  price as "Exchange Rate, USD",
  null::decimal(18,6) as "Close Rate, RUB",
  null::decimal(18,6) as "Close Rate, USD"
from
  {{ ref('mart_load_telegram_p2p_trades') }}

union all

-- Telegram P2P Trades - Fee
select
  date(completed_at) as "Date",
  'Telegram Wallet' as "Platform Name",
  'Fee' as "Category",
  'P2P trading fee' as "Description",
  -1.0 * paid_fee_crypto_amt as "Amount, Currency",
  crypto_currency as "Currency",
  null::decimal(18,2) as "Amount, RUB",
  null::decimal(18,2) as "Amount, USD",
  null::decimal(18,6) as "Executed Rate, RUB",
  null::decimal(18,6) as "Exchange Rate, USD",
  null::decimal(18,6) as "Close Rate, RUB",
  null::decimal(18,6) as "Close Rate, USD"
from
  {{ ref('mart_load_telegram_p2p_trades') }}
where
  paid_fee_crypto_amt > 0

union all

-- Binance Fiat Orders
select
  date(updated_at) as "Date",
  'Binance' as "Platform Name",
  'Fiat ' || transaction_type as "Category",
  transaction_type || ' ' || cast(indicated_amt as varchar) || ' via ' || method as "Description",
  case when transaction_type = 'Deposit' then amount else -1.0 * amount end as "Amount, Currency",
  fiat_currency as "Currency",
  null::decimal(18,2) as "Amount, RUB",
  case when fiat_currency = 'USD' then (case when transaction_type = 'Deposit' then amount else -1.0 * amount end) else null end as "Amount, USD",
  null::decimal(18,6) as "Executed Rate, RUB",
  null::decimal(18,6) as "Exchange Rate, USD",
  null::decimal(18,6) as "Close Rate, RUB",
  null::decimal(18,6) as "Close Rate, USD"
from
  {{ ref('mart_load_binance_fiat_orders') }}

union all

-- Binance Fiat Orders - Fee
select
  date(updated_at) as "Date",
  'Binance' as "Platform Name",
  'Fee' as "Category",
  'Fiat ' || transaction_type || ' fee via ' || method as "Description",
  -1.0 * total_fee as "Amount, Currency",
  fiat_currency as "Currency",
  null::decimal(18,2) as "Amount, RUB",
  case when fiat_currency = 'USD' then -1.0 * total_fee else null end as "Amount, USD",
  null::decimal(18,6) as "Executed Rate, RUB",
  null::decimal(18,6) as "Exchange Rate, USD",
  null::decimal(18,6) as "Close Rate, RUB",
  null::decimal(18,6) as "Close Rate, USD"
from
  {{ ref('mart_load_binance_fiat_orders') }}
where
  total_fee > 0

union all

-- Bybit Internal Transfers
select
  date(success_at) as "Date",
  'Bybit' as "Platform Name",
  'Internal Transfer' as "Category",
  'Transfer ' || cast(amount as varchar) || ' ' || coin || ' from ' || from_account_type || ' to ' || to_account_type as "Description",
  amount as "Amount, Currency",
  coin as "Currency",
  null::decimal(18,2) as "Amount, RUB",
  case when coin in ('USDT', 'USDC', 'USD') then amount else null end as "Amount, USD",
  null::decimal(18,6) as "Executed Rate, RUB",
  null::decimal(18,6) as "Exchange Rate, USD",
  null::decimal(18,6) as "Close Rate, RUB",
  null::decimal(18,6) as "Close Rate, USD"
from
  {{ ref('mart_load_bybit_internal_transfers') }}

union all

-- Bybit Convert Trades - From (what you gave up)
select
  date(created_at) as "Date",
  'Bybit' as "Platform Name",
  'Convert' as "Category",
  'Convert ' || cast(from_amt as varchar) || ' ' || from_coin || ' to ' || cast(to_amt as varchar) || ' ' || to_coin || ' - From' as "Description",
  -1.0 * from_amt as "Amount, Currency",
  from_coin as "Currency",
  null::decimal(18,2) as "Amount, RUB",
  case when from_coin in ('USDT', 'USDC', 'USD') then -1.0 * from_amt else null end as "Amount, USD",
  null::decimal(18,6) as "Executed Rate, RUB",
  exchange_rate as "Exchange Rate, USD",
  null::decimal(18,6) as "Close Rate, RUB",
  null::decimal(18,6) as "Close Rate, USD"
from
  {{ ref('mart_load_bybit_convert_trades') }}

union all

-- Bybit Convert Trades - To (what you received)
select
  date(created_at) as "Date",
  'Bybit' as "Platform Name",
  'Convert' as "Category",
  'Convert ' || cast(from_amt as varchar) || ' ' || from_coin || ' to ' || cast(to_amt as varchar) || ' ' || to_coin || ' - To' as "Description",
  to_amt as "Amount, Currency",
  to_coin as "Currency",
  null::decimal(18,2) as "Amount, RUB",
  case when to_coin in ('USDT', 'USDC', 'USD') then to_amt else null end as "Amount, USD",
  null::decimal(18,6) as "Executed Rate, RUB",
  exchange_rate as "Exchange Rate, USD",
  null::decimal(18,6) as "Close Rate, RUB",
  null::decimal(18,6) as "Close Rate, USD"
from
  {{ ref('mart_load_bybit_convert_trades') }}

union all

-- Binance Fiat Payments - Fiat side
select
  date(updated_at) as "Date",
  'Binance' as "Platform Name",
  'Fiat Payment' as "Category",
  transaction_type || ' ' || cast(obtain_amt as varchar) || ' ' || crypto_currency || ' for ' || cast(source_amt as varchar) || ' ' || fiat_currency || ' via ' || payment_method || ' - Fiat' as "Description",
  case
    when transaction_type = 'Buy' then -1.0 * source_amt  -- When buying crypto, you pay fiat
    when transaction_type = 'Sell' then source_amt  -- When selling crypto, you receive fiat
  end as "Amount, Currency",
  fiat_currency as "Currency",
  null::decimal(18,2) as "Amount, RUB",
  case
    when fiat_currency = 'USD' and transaction_type = 'Buy' then -1.0 * source_amt
    when fiat_currency = 'USD' and transaction_type = 'Sell' then source_amt
    else null
  end as "Amount, USD",
  null::decimal(18,6) as "Executed Rate, RUB",
  price as "Exchange Rate, USD",
  null::decimal(18,6) as "Close Rate, RUB",
  null::decimal(18,6) as "Close Rate, USD"
from
  {{ ref('mart_load_binance_fiat_payments') }}

union all

-- Binance Fiat Payments - Crypto side
select
  date(updated_at) as "Date",
  'Binance' as "Platform Name",
  'Fiat Payment' as "Category",
  transaction_type || ' ' || cast(obtain_amt as varchar) || ' ' || crypto_currency || ' for ' || cast(source_amt as varchar) || ' ' || fiat_currency || ' via ' || payment_method || ' - Crypto' as "Description",
  case
    when transaction_type = 'Buy' then obtain_amt  -- When buying crypto, you receive crypto
    when transaction_type = 'Sell' then -1.0 * obtain_amt  -- When selling crypto, you give up crypto
  end as "Amount, Currency",
  crypto_currency as "Currency",
  null::decimal(18,2) as "Amount, RUB",
  null::decimal(18,2) as "Amount, USD",
  null::decimal(18,6) as "Executed Rate, RUB",
  price as "Exchange Rate, USD",
  null::decimal(18,6) as "Close Rate, RUB",
  null::decimal(18,6) as "Close Rate, USD"
from
  {{ ref('mart_load_binance_fiat_payments') }}

union all

-- Binance Fiat Payments - Fee
select
  date(updated_at) as "Date",
  'Binance' as "Platform Name",
  'Fee' as "Category",
  'Fiat payment fee via ' || payment_method as "Description",
  -1.0 * total_fee as "Amount, Currency",
  fiat_currency as "Currency",
  null::decimal(18,2) as "Amount, RUB",
  case when fiat_currency = 'USD' then -1.0 * total_fee else null end as "Amount, USD",
  null::decimal(18,6) as "Executed Rate, RUB",
  null::decimal(18,6) as "Exchange Rate, USD",
  null::decimal(18,6) as "Close Rate, RUB",
  null::decimal(18,6) as "Close Rate, USD"
from
  {{ ref('mart_load_binance_fiat_payments') }}
where
  total_fee > 0

union all

-- Bybit Earn Yield (Staking/APY yield distributions)
select
  date(created_at_utc) as "Date",
  created_at_utc as "Transaction Timestamp",
  'Bybit' as "Platform Name",
  'Yield' as "Category",
  'Earn yield distribution (' || yieldType || ')' as "Description",
  amount as "Amount, Currency",
  coin as "Currency",
  null::decimal(18,2) as "Amount, RUB",
  case when coin in ('USDT', 'USDC', 'USD') then amount else null end as "Amount, USD",
  null::decimal(18,6) as "Executed Rate, RUB",
  null::decimal(18,6) as "Exchange Rate, USD",
  null::decimal(18,6) as "Close Rate, RUB",
  null::decimal(18,6) as "Close Rate, USD"
from
  {{ ref('mart_load_bybit_earn_yield') }}
