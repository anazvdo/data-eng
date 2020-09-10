select
    count(*)
from
    products
WHERE
    ymd = '{{ ds_nodash }}'
    AND product_id is null