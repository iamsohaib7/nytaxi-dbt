{{ config(materialized='table') }}

select * from (
  values
  (1, 'Credit card'),
  (2, 'Cash'),
  (3, 'No charge'),
  (4, 'Dispute'),
  (5, 'Unknown'),
  (6, 'Voided trip')
) as t(payment_type, payment_desc)
