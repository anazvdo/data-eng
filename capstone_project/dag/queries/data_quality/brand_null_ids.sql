select
    count(*)
from
    brand
where
    ymd = '{{ ds_nodash }}'
    and brand_id is null