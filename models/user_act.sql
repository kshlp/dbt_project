{{
  config(
    materialized='incremental',
    unique_key='user_id',
    incremental_strategy='insert_overwrite'
  )
}}

WITH recent_orders AS (
  SELECT
    user_id,
    COUNT(order_id) AS order_count,
    SUM(order_amount) AS total_amount,
    AVG(order_amount) AS average_amount,
    MAX(order_date) AS last_order_date
  FROM {{ source('example_source', 'orders') }}  
  WHERE 
    {% if is_incremental() %}
      order_date >= DATE('now', '-7 days')
    {% else %}
      1=1
    {% endif %}
  GROUP BY user_id
)

SELECT
  u.user_id,
  u.registration_date,
  COALESCE(ro.order_count, 0) AS order_count,
  COALESCE(ro.total_amount, 0.0) AS total_amount,
  COALESCE(ro.average_amount, 0.0) AS average_amount,
  CASE
    WHEN ro.last_order_date >= DATE('now', '-7 days') THEN 'Active'
    WHEN ro.last_order_date IS NULL THEN 'New'
    ELSE 'Inactive'
  END AS user_status,
  ro.last_order_date
FROM {{ source('example_source', 'users') }} u
LEFT JOIN recent_orders ro ON u.user_id = ro.user_id
