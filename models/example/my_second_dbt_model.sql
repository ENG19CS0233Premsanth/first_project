-- Configuration
{% if target.name == 'dev' %}
    {{ config(
        schema='DEV',
        materialized='table',
        database='DBT_DEV'
    ) }}
{% elif target.name == 'qa' %}
    {{ config(
        materialized='table',
        schema='QA',
        database='DBT_QA'
    ) }}
{% endif %}

-- Setting parameters
{% set sales_date = '2000-01-03' %}
{% set limit = 100 %}

-- SQL code starts here
WITH store_sales_cte AS (
    SELECT * FROM {{ source('snowflake_sample_data2', 'store_sales') }}
),
catalog_sales_cte AS (
    SELECT * FROM {{ source('snowflake_sample_data2', 'catalog_sales') }}
),
web_sales_cte AS (
    SELECT * FROM {{ source('snowflake_sample_data2', 'web_sales') }}
),
date_dim_cte AS (
    SELECT * FROM {{ source('snowflake_sample_data2', 'date_dim') }}
),
item_cte AS (
    SELECT * FROM {{ source('snowflake_sample_data2', 'item') }}
),
date_filtered_cte AS (
    SELECT d_date
    FROM date_dim_cte
    WHERE d_week_seq = (
        SELECT d_week_seq
        FROM date_dim_cte
        WHERE d_date = CAST('{{ sales_date }}' AS DATE)
    )
),
ss_date_join AS (
    SELECT 
        ss.*, 
        dd.d_date 
    FROM 
        store_sales_cte ss
    JOIN 
        date_dim_cte dd ON ss.ss_sold_date_sk = dd.d_date_sk
),
ss_item_join AS (
    SELECT 
        sdj.*, 
        i.i_item_id 
    FROM 
        ss_date_join sdj
    JOIN 
        item_cte i ON sdj.ss_item_sk = i.i_item_sk
),
ss_items AS (
    SELECT
        i_item_id AS item_id,
        SUM(ss_ext_sales_price) AS ss_item_rev
    FROM
        ss_item_join
    WHERE
        d_date IN (SELECT d_date FROM date_filtered_cte)
    GROUP BY
        i_item_id
),
cs_date_join AS (
    SELECT 
        cs.*, 
        dd.d_date 
    FROM 
        catalog_sales_cte cs
    JOIN 
        date_dim_cte dd ON cs.cs_sold_date_sk = dd.d_date_sk
),
cs_item_join AS (
    SELECT 
        cdj.*, 
        i.i_item_id 
    FROM 
        cs_date_join cdj
    JOIN 
        item_cte i ON cdj.cs_item_sk = i.i_item_sk
),
cs_items AS (
    SELECT
        i_item_id AS item_id,
        SUM(cs_ext_sales_price) AS cs_item_rev
    FROM
        cs_item_join
    WHERE
        d_date IN (SELECT d_date FROM date_filtered_cte)
    GROUP BY
        i_item_id
),
ws_date_join AS (
    SELECT 
        ws.*, 
        dd.d_date 
    FROM 
        web_sales_cte ws
    JOIN 
        date_dim_cte dd ON ws.ws_sold_date_sk = dd.d_date_sk
),
ws_item_join AS (
    SELECT 
        wdj.*, 
        i.i_item_id 
    FROM 
        ws_date_join wdj
    JOIN 
        item_cte i ON wdj.ws_item_sk = i.i_item_sk
),
ws_items AS (
    SELECT
        i_item_id AS item_id,
        SUM(ws_ext_sales_price) AS ws_item_rev
    FROM
        ws_item_join
    WHERE
        d_date IN (SELECT d_date FROM date_filtered_cte)
    GROUP BY
        i_item_id
)
SELECT
    ss_items.item_id,
    ss_item_rev,
    ss_item_rev / ((ss_item_rev + cs_item_rev + ws_item_rev) / 3) * 100 AS ss_dev,
    cs_item_rev,
    cs_item_rev / ((ss_item_rev + cs_item_rev + ws_item_rev) / 3) * 100 AS cs_dev,
    ws_item_rev,
    ws_item_rev / ((ss_item_rev + cs_item_rev + ws_item_rev) / 3) * 100 AS ws_dev,
    (ss_item_rev + cs_item_rev + ws_item_rev) / 3 AS average
FROM
    ss_items
JOIN
    cs_items ON ss_items.item_id = cs_items.item_id
JOIN
    ws_items ON ss_items.item_id = ws_items.item_id
WHERE
    ss_item_rev BETWEEN 0.9 * cs_item_rev AND 1.1 * cs_item_rev
    AND ss_item_rev BETWEEN 0.9 * ws_item_rev AND 1.1 * ws_item_rev
    AND cs_item_rev BETWEEN 0.9 * ss_item_rev AND 1.1 * ss_item_rev
    AND cs_item_rev BETWEEN 0.9 * ws_item_rev AND 1.1 * ws_item_rev
    AND ws_item_rev BETWEEN 0.9 * ss_item_rev AND 1.1 * ss_item_rev
    AND ws_item_rev BETWEEN 0.9 * cs_item_rev AND 1.1 * cs_item_rev
ORDER BY
    item_id,
    ss_item_rev
LIMIT
    {{ limit }}
