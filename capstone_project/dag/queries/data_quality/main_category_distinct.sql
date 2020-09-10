SELECT
    CASE
        WHEN total = distinct_values THEN true
        ELSE false
    END
FROM
    (
        SELECT
            count(*) as total,
            count(distinct main_cat_id) as distinct_values
        FROM
            main_category
        WHERE ymd='{{ ds_nodash }}'
    )