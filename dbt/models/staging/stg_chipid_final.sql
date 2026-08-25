SELECT * EXCLUDE (rn) FROM (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY lot_id, efuse_raw ORDER BY retest_num DESC
    ) AS rn FROM {{ source('stdf', 'chipid') }}
) WHERE rn = 1
