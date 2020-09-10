SELECT
    CASE
        WHEN total = distinct_values THEN true
        ELSE false
    END
FROM
    (
        SELECT
            count(*) AS total,
            count(distinct timestamp) AS distinct_values
        FROM
            time
        WHERE ymd='{{ ds_nodash }}'
    )