SELECT
    CASE
        WHEN total = distinct_values THEN true
        ELSE false
    END
FROM
    (
        SELECT
            count(*) total,
            count(distinct concat(reviewer_id, product_id)) as distinct_values
        FROM
            ratings
        WHERE ymd='{{ ds_nodash }}'
    )