select
  't_bank' as bank_nm,
  category_nm,
  description,
  transaction_amt,
  transaction_currency_cd,
  transaction_dt
from
  {{ ref('mart_load_t_bank_transactions') }}

union all

select
  't_bank' as bank_nm,
  'Cashback' as category_nm,
  'Cashback and other rewards' as description,
  total_rewards_amt as transaction_amt,
  transaction_currency_cd,
  transaction_dt
from
  {{ ref('mart_load_t_bank_transactions') }}
where
  total_rewards_amt > 0
