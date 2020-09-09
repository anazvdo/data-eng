SELECT
    CASE
        WHEN total = distinct_values THEN true
        ELSE false
    END
FROM
    (
        SELECT
            count(*),
            count(distinct main_cat_id)
        FROM
            main_category
    )