-- cli.py verify-flags の inconsistent_runs 不変条件: 同一 run 内でフラグが割れているキーは存在しない。
SELECT {{ dedup_key() }}, retest_num
FROM {{ source('stdf', 'test_data') }}
GROUP BY {{ dedup_key() }}, retest_num
HAVING MIN(retest_flag) != MAX(retest_flag)
