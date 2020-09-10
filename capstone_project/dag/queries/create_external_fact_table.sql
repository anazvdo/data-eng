CREATE EXTERNAL TABLE `product_ratings`(
  `product_id` string, 
  `title` string,
  `rating` double,
  `reviewer_id` string,
  `price` double,
  `brand` string,
  `category` string,
  `main_category` string,
  `year` int,
  `month` int,
  `day` int)
PARTITIONED BY(
  `ymd` string  
)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://capstone-project-data/data_modeling/fact_table/product_ratings'