select
  transaction_bk,
  't_bank' as bank_nm,
  category_nm,
  description,
  transaction_amt,
  transaction_currency_cd,
  transacted_at_utc,
  transaction_dt,
  total_rewards_amt,
  __ingested_at
from
  {{ ref('mart_load_t_bank_transactions') }}
