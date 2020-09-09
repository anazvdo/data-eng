SELECT
    CASE
        WHEN total = distinct_values THEN true
        ELSE false
    END
FROM
    (
        SELECT
            count(*),
            count(distinct concat(reviewer_id, product_id))
        FROM
            ratings
    )