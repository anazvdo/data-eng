select
    count(*)
from
    category
where
    ymd = '{{ ds_nodash }}'
    and category_id is null