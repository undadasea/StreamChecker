import pyspark.sql

#url = jdbc:postgresql://localhost:5432/<db_name>
def readDB(spark):
    url = "jdbc:postgresql://localhost:5432/traffic_limits?user=postgres&password=newpassword"

    dataframe = spark.read \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://localhost:5432/traffic_limits") \
            .option("dbtable", "limits_per_hour") \
            .option("user", "postgres") \
            .option("password", "newpassword") \
            .load()

    dataframe.show()
    return dataframe
