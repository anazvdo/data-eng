SELECT
    CASE
        WHEN total = distinct_values THEN true
        ELSE false
    END
FROM
    (
        SELECT
            count(*),
            count(distinct brand_id)
        FROM
            brand
    )