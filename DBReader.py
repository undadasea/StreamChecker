import pyspark.sql
from pyspark.sql.types import *

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
    dataframe.registerTempTable("df_table")
    limits = spark.sql("SELECT limit_name, limit_value, MAX(effective_date) as last_date "+ \
                        "FROM df_table GROUP BY limit_name, limit_value")

    log_file = open('./log_file', 'a')
    log_file.write("Limits type: " + str(type(limits)))
    #log_file.write("Min: " + str(limits.limit_value['Minimum'])
    log_file.close()

    return limits

def createDBstream(spark):
    schema = StructType([ StructField("limit_name", StringType(), True),
                          StructField("limit_value", IntegerType(), True),
                          StructField("effective_date", TimestampType(), True) ])

    streamingDF = (spark.readStream.schema(schema).option("maxFilesPerTrigger", 1) \
                        .json("/home/undadasea/StreamChecker/traffic_limits.json"))
    return streamingDF
