-- cli.py verify-flags の orphaned_keys 不変条件: newest run が flag 0 でないキーは存在しない。
SELECT {{ dedup_key() }}
FROM {{ source('stdf', 'test_data') }}
GROUP BY {{ dedup_key() }}
HAVING MIN(retest_flag) != 0
