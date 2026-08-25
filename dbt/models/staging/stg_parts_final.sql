SELECT * EXCLUDE (rn) FROM (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY lot_id, wafer_id, x_coord, y_coord,
            CASE WHEN x_coord = -32768 AND y_coord = -32768 THEN part_txt ELSE '' END
        ORDER BY retest_num DESC
    ) AS rn FROM {{ source('stdf', 'parts') }}
) WHERE rn = 1
