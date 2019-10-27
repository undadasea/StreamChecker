from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

# this module reads data from kafka topic and process it
# start using ./bin/spark-submit --jars spark-streaming-kafka-0-8-assembly_2.11-2.4.4.jar /home/undadasea/StreamProcessing.py
def main():
    topic = "new_topic"
    brokerAddresses = "localhost:9092"
    batchTime = 20
    
    spark = SparkSession.builder.appName("StreamProcessing").getOrCreate()
    sc = spark.sparkContext
    ssc = StreamingContext(sc, batchTime)    
    kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokerAddresses})

    # TODO: kvs.map(lambda l : l.lower())
    kvs.pprint() # printing result on stdout
    
    # starting the task run.
    ssc.start()
    ssc.awaitTermination()

if __name__ == '__main__':
    main()
