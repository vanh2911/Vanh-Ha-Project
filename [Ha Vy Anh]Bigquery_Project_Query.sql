-- Big project for SQL
-- Link instruction: https://docs.google.com/spreadsheets/d/1WnBJsZXj_4FDi2DyfLH1jkWtfTridO2icWbWCh7PLs8/edit#gid=0


-- Query 01: calculate total visit, pageview, transaction and revenue for Jan, Feb and March 2017 order by month
#standardSQL
SELECT 
  LEFT(FORMAT_DATE('%Y%m%d',PARSE_DATE('%Y%m%d',date)),6) as month,
  sum(totals.visits) as visits,
  sum(totals.pageviews) as pageview,
  sum(totals.transactions)as transaction,
  sum(totals.totalTransactionRevenue)/1000000 as revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*` 
WHERE _table_suffix between '0101' and '0331'
GROUP BY month
ORDER BY month

-- Query 02: Bounce rate per traffic source in July 2017
#standardSQL
-- bounce rate per traffic source in 201707
-- bounce rate = (num_bounce/total_visit)

SELECT
  trafficSource.source as source,
  SUM(totals.bounces) as total_no_of_bounce,
  SUM(totals.visits) as total_visit,
  ROUND(100*SUM(totals.bounces)/SUM(totals.visits),2) as bounce_rate
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
GROUP BY trafficSource.source
ORDER BY total_visit DESC

-- Query 3: Revenue by traffic source by week, by month in June 2017

--Revenue by traffic source by week, by month in June 2017
With week_revenue 
as(
  SELECT 
    'Week' as time_type,
    FORMAT_DATE('%Y%V',PARSE_DATE('%Y%m%d',date)) as time,
    trafficSource.source as source,
    sum(totals.totalTransactionRevenue)/1000000 as revenue
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
  WHERE _table_suffix BETWEEN '0601' AND '0630'
  GROUP BY 
    time_type,
    time,
    source
  ORDER BY time_type, revenue DESC)
,
month_revenue 
as(
  SELECT 
    'Month' as time_type,
    LEFT(FORMAT_DATE('%Y%m%d',PARSE_DATE('%Y%m%d',date)),6) as time,
    trafficSource.source as source,
    sum(totals.totalTransactionRevenue)/1000000 as revenue
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
  WHERE _table_suffix BETWEEN '0601' AND '0630'
  GROUP BY
     time_type,
      time,
      source
  order by revenue DESC)

SELECT *
FROM month_revenue
UNION ALL
SELECT *
FROM week_revenue
order by revenue desc

--Query 04: Average number of product pageviews by purchaser type (purchasers vs non-purchasers) in June, July 2017. Note: totals.transactions >=1 for purchaser and totals.transactions is null for non-purchaser
#standardSQL

WITH purchase 
as(
    SELECT 
         LEFT(FORMAT_DATE('%Y%m%d',PARSE_DATE('%Y%m%d',date)),6) as month,
        ROUND(SUM(totals.pageviews)/COUNT(DISTINCT fullVisitorId),2)as avg_pageviews_purchase
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
  WHERE _table_suffix BETWEEN '0601' AND '0731'
  AND totals.transactions >0
  GROUP BY month 
  ORDER BY avg_pageviews_purchase),

non_purchase
 as(
    SELECT 
        LEFT(FORMAT_DATE('%Y%m%d',PARSE_DATE('%Y%m%d',date)),6) as month,
        ROUND(SUM(totals.pageviews)/COUNT(DISTINCT fullVisitorId),2)as avg_pageviews_non_purchase
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*` 
  WHERE _table_suffix BETWEEN '0601' AND '0731'
  AND totals.transactions IS NULL
  GROUP BY month
  ORDER BY avg_pageviews_non_purchase)

SELECT
  p.month,
  avg_pageviews_purchase,
  avg_pageviews_non_purchase
FROM purchase as p
JOIN non_purchase as n
ON p.month = n.month
ORDER BY p.month


-- Query 05: Average number of transactions per user that made a purchase in July 2017
#standardSQL
SELECT 
  FORMAT_DATE('%Y%m',PARSE_DATE('%Y%m%d',date)) as date, 
  SUM(totals.transactions)/COUNT(distinct fullVisitorId) as Avg_total_transactions_per_user
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*` 
WHERE _table_suffix BETWEEN '0701' AND '0731'
AND totals.transactions >0
GROUP BY date


-- Query 06: Average amount of money spent per session
#standardSQL
SELECT 
    LEFT(FORMAT_DATE('%Y%m%d',PARSE_DATE('%Y%m%d',date)),6) as date,
    (SUM(totals.totalTransactionRevenue)/1000000)
  /
    SUM(totals.visits) as Avg_total_transactions_per_user
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*` 
WHERE _table_suffix BETWEEN '0701' AND '0731'
AND totals.transactions IS NOT NULL
GROUP BY date


-- Query 07: Other products purchased by customers who purchased product "YouTube Men's Vintage Henley" in July 2017. Output should show product name and the quantity was ordered.
#standardSQL
--mình phải tìm những id đã mua áo youtube này, ở trong bảng không có áo youtube để tìm xem những sản phẩm khác mà người đó mua

SELECT 
  product.v2productName as product_name,
  SUM(product.productQuantity) as total_quantity
FROM 
  `bigquery-public-data.google_analytics_sample.ga_sessions_201707*` as ga,
  unnest(hits) as hits,
  unnest(product) as product
WHERE fullVisitorId IN
(SELECT fullVisitorId
FROM
  `bigquery-public-data.google_analytics_sample.ga_sessions_201707*` as sub,
  UNNEST (hits) hits,
  UNNEST (hits.product) product
where product.v2ProductName="YouTube Men's Vintage Henley"
AND product.productRevenue >0
AND totals.transactions >0)
AND product.productRevenue >0
AND product.v2ProductName <>"YouTube Men's Vintage Henley"
GROUP BY product_name
ORDER BY total_quantity desc


--Query 08: Calculate cohort map from pageview to addtocart to purchase in last 3 month. For example, 100% pageview then 40% add_to_cart and 10% purchase.
#standardSQL
-- đầu tiên mình tìm num product view, mình sẽ chia ra làm 5 phần và join lại với nhau
with product_view as(
  SELECT 
    LEFT(FORMAT_DATE('%Y%m%d',PARSE_DATE('%Y%m%d',date)),6) as date,
    COUNT(product.v2ProductName) as num_product_view
  FROM  
    `bigquery-public-data.google_analytics_sample.ga_sessions_2017*` as ga,
    UNNEST (hits) hits,
    UNNEST (hits.product) product
  WHERE _table_suffix BETWEEN '0101' AND '0331' 
  AND ecommerceaction.action_type = '2'
  GROUP BY date
  ORDER BY date),

  add_to_cart as(
  SELECT 
    LEFT(FORMAT_DATE('%Y%m%d',PARSE_DATE('%Y%m%d',date)),6) as date,
    COUNT(product.v2ProductName) as num_add_to_cart
  FROM  
    `bigquery-public-data.google_analytics_sample.ga_sessions_2017*` as ga,
    UNNEST (hits) hits,
    UNNEST (hits.product) product
    WHERE _table_suffix BETWEEN '0101' AND '0331' 
  AND ecommerceaction.action_type = '3'
  GROUP BY date
  ORDER BY date),

purchase as(
   SELECT 
    LEFT(FORMAT_DATE('%Y%m%d',PARSE_DATE('%Y%m%d',date)),6) as date,
    COUNT(product.v2ProductName) as num_purchase
  FROM  
    `bigquery-public-data.google_analytics_sample.ga_sessions_2017*` as ga,
    UNNEST (hits) hits,
    UNNEST (hits.product) product
    WHERE _table_suffix BETWEEN '0101' AND '0331' 
  AND ecommerceaction.action_type = '6'
  GROUP BY date
  ORDER BY date)

  SELECT 
    c.date
    num_product_view,
    num_add_to_cart,
    num_purchase,
    ROUND(100*(num_add_to_cart/num_product_view),2) as add_to_cart_rate,
    ROUND(100*(num_purchase/num_product_view),2) as purchase_rate
  FROM product_view as v
  JOIN add_to_cart as c ON v.date=c.date
  JOIN purchase as p ON p.date=c.date
  ORDER BY c.date 