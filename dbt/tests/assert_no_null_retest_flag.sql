-- cli.py verify-flags の null_flags 不変条件: retest_flag が NULL の行は存在しない。
SELECT lot_id, COUNT(*) AS null_flags
FROM {{ source('stdf', 'test_data') }}
WHERE retest_flag IS NULL
GROUP BY lot_id
