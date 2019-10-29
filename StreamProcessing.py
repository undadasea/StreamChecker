import argparse

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import functions as F

from DBReader import readDB, createDBstream

TOPIC_NAME = "new_topic"
HOST_PORT = "localhost:9092"

# this module reads data from kafka topic and processes it
# start using ./bin/spark-submit --jars spark-streaming-kafka-0-8-assembly_2.11-2.4.4.jar --driver-class-path /home/undadasea/postgresql-42.2.8.jar /home/undadasea/StreamChecker/StreamProcessing.py
def get_str_bytes_ip(ip=None):
    if ip:
        addr_array = ip.split(".")
        for i in range(4):
            addr_array[i] = "{0:#0{1}x}".format(int(addr_array[i]),4)

        return "\\"+addr_array[0]+"\\"+ \
                    addr_array[1]+"\\"+ \
                    addr_array[2]+"\\"+ \
                    addr_array[3]
    else:
        return ""


def main(source_ip=None, destination_ip=None):
    topic = TOPIC_NAME
    brokerAddresses = HOST_PORT
    batchTime = 60*5 # in seconds

    spark = SparkSession.builder.appName("StreamProcessing").getOrCreate()
    sc = spark.sparkContext
    ssc = StreamingContext(sc, batchTime)
    kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokerAddresses})

    # sql dataframe stream
    limits = createDBstream(spark)
    # the query writes all new data every time there are updates
    query = (limits.agg(F.max(limits.effective_date)).collect().writeStream.format("memory").queryName("query").outputMode("update").start())

    kvs.pprint()

    source = get_str_bytes_ip(source_ip)
    filtered_traffic = kvs.filter(lambda x: str(x[0])[:20] == source)

    dest = get_str_bytes_ip(destination_ip)
    filtered_traffic = filtered_traffic.filter(lambda x: str(x[0])[20:] == dest)

    sum_traffic = filtered_traffic.map(lambda x: int(x[1]))
    sum = sum_traffic.reduce(lambda a, b: a+b)
    sum.pprint()

    # starting the task run.
    ssc.start()
    ssc.awaitTermination()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", help="to only count traffic from this source address")
    parser.add_argument("--destination", help="to only count traffic from this dest address")
    args = parser.parse_args()

    main(source_ip=args.source, destination_ip=args.destination)
