select
    count(*)
from
    time
WHERE
    ymd = '{{ ds_nodash }}'
    AND timestamp is null