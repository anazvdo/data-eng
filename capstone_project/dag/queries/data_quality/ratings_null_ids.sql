select
    count(*)
from
    ratings
WHERE
    ymd = '{{ ds_nodash }}'
    AND reviewer_id is null