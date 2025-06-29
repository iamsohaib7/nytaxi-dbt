{{ config(materialized='table') }}

select * from (
  values 
  (1, 'CMT', 'Creative Mobile Technologies, LLC'),
  (2, 'VTS', 'VeriFone Inc.'),
  (99, 'Unknown', 'Unknown Vendor')
) as t(vendorid, vendor_short_name, vendor_full_name)