SELECT lot_id, product, test_category, sub_process,
       arg_max(part_type,   start_time) AS part_type,
       arg_max(job_name,    start_time) AS job_name,
       arg_max(job_rev,     start_time) AS job_rev,
       MIN(start_time)  AS start_time,
       MAX(finish_time) AS finish_time,
       arg_max(tester_type, start_time) AS tester_type,
       arg_max(operator,    start_time) AS operator,
       COUNT(DISTINCT (job_name, job_rev))     AS job_variant_count,
       COUNT(DISTINCT (job_name, job_rev)) > 1 AS job_mixed
FROM {{ source('stdf', 'runs') }}
GROUP BY lot_id, product, test_category, sub_process
