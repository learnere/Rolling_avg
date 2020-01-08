# Rolling_avg
To run this code in Python IDE: 
1. Down load the needed data at https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page
2. put the data in directory data/input/FILE_NAME  
  *for this code specifically: FILE_NAME = yellow_tripdata_2019-01.csv


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
schema = StructType([structField('id', LongType(), True),StructField('prediction', DoubleType(), True)]) 
```                      

4. Define the grouped map Pandas User Defined Function using @pandas_udf to annotate the Python functions that compose the pipeline
```python
@pandas_udf (schema, PandasUDFType.GROUPED_MAP) 
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
result = pipeline.groupby('id').apply(spark_df)
```
