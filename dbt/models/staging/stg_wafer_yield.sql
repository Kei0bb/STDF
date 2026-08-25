{% set gd_map = var('gross_die_map', {}) %}
WITH gross_die AS (
    {% if gd_map %}
    SELECT * FROM (VALUES
        {% for product, cfg in gd_map.items() %}
        ('{{ product }}', {{ cfg['gross_die'] }}, {{ cfg['gd_fail_bin'] }}){{ "," if not loop.last }}
        {% endfor %}
    ) AS t(product, gross_die, gd_fail_bin)
    {% else %}
    SELECT CAST(NULL AS VARCHAR) AS product, CAST(NULL AS BIGINT) AS gross_die,
           CAST(NULL AS BIGINT) AS gd_fail_bin WHERE FALSE
    {% endif %}
),
probed AS (
    SELECT lot_id, wafer_id,
           COUNT(*)                                AS probed,
           SUM(CASE WHEN passed THEN 1 ELSE 0 END) AS good
    FROM {{ ref('stg_parts_final') }}
    GROUP BY lot_id, wafer_id
),
lp AS (
    SELECT lot_id, ANY_VALUE(product) AS product FROM {{ ref('stg_lots') }} GROUP BY lot_id
),
joined AS (
    SELECT pr.lot_id, pr.wafer_id, pr.probed, pr.good,
           CASE WHEN pr.wafer_id <> '' AND gd.gross_die IS NOT NULL
                THEN GREATEST(pr.probed, gd.gross_die)
                ELSE pr.probed END AS total,
           gd.gd_fail_bin
    FROM probed pr
    LEFT JOIN lp ON pr.lot_id = lp.lot_id
    LEFT JOIN gross_die gd ON lp.product = gd.product
)
SELECT lot_id, wafer_id, probed, good, total,
       total - probed AS unprobed,
       gd_fail_bin,
       ROUND(100.0 * good / NULLIF(total, 0), 2) AS yield_pct
FROM joined
