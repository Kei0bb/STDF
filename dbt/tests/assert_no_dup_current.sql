-- cli.py verify-flags の dup_current 不変条件: flag 0 が複数 retest_num に跨るキーは存在しない。
SELECT {{ dedup_key() }}
FROM {{ source('stdf', 'test_data') }}
WHERE retest_flag = 0
GROUP BY {{ dedup_key() }}
HAVING COUNT(DISTINCT retest_num) > 1
