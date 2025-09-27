select
  card_last4,
  status_nm,
  category_nm,
  description,
  transaction_amt,
  transaction_currency_cd,
  transacted_at_utc,
  total_rewards_amt,
  current_timestamp at time zone 'UTC' AS __ingested_at
from
  {{ ref('stg_t_bank_transactions') }}
