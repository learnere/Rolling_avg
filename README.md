# Rolling_avg
To run this code in Python IDE: 
1. Down load the needed data at page https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page ->2019->January->Yellow Taxi Trip Recods (CSV)
2. You should get a file named yellow_tripdata_2019-01.csv 
3. Put the data in directory data/input/FILE_NAME  
  **DO NOT CHANGE THE FILE NAME**
4. The code named avg_trip_length.py calculates the average trip length of all Yellow Taxis for a month.
5. The code named rolling_avg_trip_length.py can ingest new data (data/input/new_data.csv)
   and calculates the 45 day rolling average trip length.

# To scale the pipeline to a multiple of the data size that does not fit any more to one machine can be done by following these steps:

1. Create a SparkContext for executing operations in a cluster.
```python
context = SparkContext()
```

2. Creating Spark Dataframes from Pandas Dataframes as data
```python
spark_df = context.createDataFrame(pandas_df)  
```

3. Define a schema for the result set, according to the final result of the pipeline 
```python
schema = StructType([structField('id', IntegerType(), True),StructField('pickup_datetime', DateType(), True), StructField('SMA45', DoubleType(), True)]) 
```                      

4. Define the grouped map Pandas User Defined Function using @pandas_udf to annotate the Python functions (eg.get_avg_distance etc)that compose the pipeline
```python
@pandas_udf(schema, PandasUDFType.GROUPED_MAP) 
    def func_1() 
    def func_2() 
    def func_3()
```

5. Define the Pipeline with the sequence of stages
```python
pipeline = Pipeline(stages=[fun_1, fun2, fun_3])
```

6. Partition the data and run the UDF with groupBy().apply() to implement the “split-apply-combine” pattern. 
```Python
result = pipeline.groupBy('id').apply(spark_df)
```
