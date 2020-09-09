SELECT
    CASE
        WHEN total = distinct_values THEN true
        ELSE false
    END
FROM
    (
        SELECT
            count(*),
            count(distinct category_id)
        FROM
            category
    )