SELECT
    CASE
        WHEN total = distinct_values THEN true
        ELSE false
    END
FROM
    (
        SELECT
            count(*) as total,
            count(distinct category_id) as distinct_values
        FROM
            category
    )