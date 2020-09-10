select
    count(*)
from
    main_category
WHERE
    ymd = '{{ ds_nodash }}'
    AND main_cat_id is null