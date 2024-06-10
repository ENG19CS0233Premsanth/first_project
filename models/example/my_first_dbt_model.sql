-- Configuration
{% if target.name == 'dev' %}
    {{ config(
        materialized='table',
        schema='DEV',
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
{% set month = 2 %}
{% set year = 2001 %}
{% set gmt_offset = -5 %}
{% set limit = 100 %}
 
-- SQL code starts here
WITH store_sales_cte AS (
    SELECT * FROM {{ source('snowflake_sample_data', 'store_sales') }}
),
catalog_sales_cte AS (
    SELECT * FROM {{ source('snowflake_sample_data', 'catalog_sales') }}
),
web_sales_cte AS (
    SELECT * FROM {{ source('snowflake_sample_data', 'web_sales') }}
),
date_dim_cte AS (
    SELECT * FROM {{ source('snowflake_sample_data', 'date_dim') }}
),
customer_address_cte AS (
    SELECT * FROM {{ source('snowflake_sample_data', 'customer_address') }}
),
item_cte AS (
    SELECT * FROM {{ source('snowflake_sample_data', 'item') }}
),
ss_joined_date AS (
    SELECT
        ss.*,
        dd.d_year,
        dd.d_moy
    FROM
        store_sales_cte ss
    JOIN
        date_dim_cte dd ON ss.ss_sold_date_sk = dd.d_date_sk
),
ss_joined_address AS (
    SELECT
        ssjd.*,
        ca.ca_gmt_offset
    FROM
        ss_joined_date ssjd
    JOIN
        customer_address_cte ca ON ssjd.ss_addr_sk = ca.ca_address_sk
),
ss AS (
    SELECT
        sja.*,
        i.i_item_id,
        i.i_color
    FROM
        ss_joined_address sja
    JOIN
        item_cte i ON sja.ss_item_sk = i.i_item_sk
    WHERE
        i.i_color IN ('slate', 'blanched', 'burnished')
        AND sja.d_year = {{ year }}
        AND sja.d_moy = {{ month }}
        AND sja.ca_gmt_offset = {{ gmt_offset }}
),
cs_joined_date AS (
    SELECT
        cs.*,
        dd.d_year,
        dd.d_moy
    FROM
        catalog_sales_cte cs
    JOIN
        date_dim_cte dd ON cs.cs_sold_date_sk = dd.d_date_sk
),
cs_joined_address AS (
    SELECT
        csjd.*,
        ca.ca_gmt_offset
    FROM
        cs_joined_date csjd
    JOIN
        customer_address_cte ca ON csjd.cs_bill_addr_sk = ca.ca_address_sk
),
cs AS (
    SELECT
        csa.*,
        i.i_item_id,
        i.i_color
    FROM
        cs_joined_address csa
    JOIN
        item_cte i ON csa.cs_item_sk = i.i_item_sk
    WHERE
        i.i_color IN ('slate', 'blanched', 'burnished')
        AND csa.d_year = {{ year }}
        AND csa.d_moy = {{ month }}
        AND csa.ca_gmt_offset = {{ gmt_offset }}
),
ws_joined_date AS (
    SELECT
        ws.*,
        dd.d_year,
        dd.d_moy
    FROM
        web_sales_cte ws
    JOIN
        date_dim_cte dd ON ws.ws_sold_date_sk = dd.d_date_sk
),
ws_joined_address AS (
    SELECT
        wsjd.*,
        ca.ca_gmt_offset
    FROM
        ws_joined_date wsjd
    JOIN
        customer_address_cte ca ON wsjd.ws_bill_addr_sk = ca.ca_address_sk
),
ws AS (
    SELECT
        wsja.*,
        i.i_item_id,
        i.i_color
    FROM
        ws_joined_address wsja
    JOIN
        item_cte i ON wsja.ws_item_sk = i.i_item_sk
    WHERE
        i.i_color IN ('slate', 'blanched', 'burnished')
        AND wsja.d_year = {{ year }}
        AND wsja.d_moy = {{ month }}
        AND wsja.ca_gmt_offset = {{ gmt_offset }}
)
SELECT
    i_item_id,
    SUM(total_sales) AS total_sales
FROM
    (
        SELECT i_item_id, SUM(ss_ext_sales_price) AS total_sales FROM ss GROUP BY i_item_id
        UNION ALL
        SELECT i_item_id, SUM(cs_ext_sales_price) AS total_sales FROM cs GROUP BY i_item_id
        UNION ALL
        SELECT i_item_id, SUM(ws_ext_sales_price) AS total_sales FROM ws GROUP BY i_item_id
    ) tmp1
GROUP BY
    i_item_id
ORDER BY
    total_sales DESC,
    i_item_id
LIMIT {{ limit }}
