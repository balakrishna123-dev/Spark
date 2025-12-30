from pyspark.sql import SparkSession

def get_spark_session(app_name):
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .getOrCreate()
    )


#Why?

#Spark session creation should be centralized

#Reusable across projects

#Clean architecture

#What happens?

#Creates Spark engine

#Uses all CPU cores (local[*])

#Returns SparkSession object