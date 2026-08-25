SELECT * FROM {{ source('stdf', 'test_data') }} WHERE retest_flag = 0
