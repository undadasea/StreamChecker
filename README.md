# Spark Streaming application
The program aims to process streams of data containing information of traffic on the local interfaces.
## Project structure
The project consists of 3 major modules: KafkaProducer, StreamProcessing and DBReader.
- KafkaProducer module has both a packet sniffer and a kafka producer. The packet sniffer simply listens on the socket for all packets going through. From each packet we only need its source ip address, destination ip address and total packet length (for measuring traffic). We process the ip header to extract this information, then we combine source and destination ip addresses to create a key and the total length of the packet becomes the value for the kafka messages to the selected topic.
- StreamProcessing module connects to the same kafka topic and receives messages from it. Using tools of pyspark.streaming we can manipulate DStreams of messages, for example, in this case we need to sum up all the traffic (values in <k,v> pairs). Also, we can sum up only values where keys satisfy our requirements such as "only sum traffic from selected source or/and destination ip". To check if the traffic of the last 5 minutes is within our set limits, we set the batch size to 60*5 seconds so that we can sum the length of all packets transmitted through by this time.
- DBReader simply creates connections to either postgresql database using jdbc driver or json data file which it uses to create a local table later.
## Installation
To run these modules you need to install zookeeper and kafka server.

To run zookeeper:

`path/to/kafka/$ bin/zookeeper-server-start.sh config/zookeeper.properties`

Run Kafka Server:

`path/to/kafka/$ bin/kafka-server-start.sh config/server.properties`

After installing run zookeeper first and then start kafka. KafkaProducer.py is run with the command

`$ python KafkaProducer.py`

(highly likely you will need to install kafka with `pip install kafka-python`). After this you can run StreamProcessing with

`./bin/spark-submit --jars /PATH_TO/spark-streaming-kafka-0-8-assembly_2.11-2.4.4.jar --driver-class-path /PATH_TO/postgresql-42.2.8.jar /PATH_TO/StreamChecker/StreamProcessing.py --source 127.0.0.1`

from the directory of downloaded spark (my spark package is spark-2.4.4-bin-hadoop2.7). Also, you may need to download dependencies spark-streaming-kafka-0-8-assembly_2.11-2.4.4.jar and postgresql-42.2.8.jar.
### Requirements satisfaction
This project can
- create a schema traffic_limits with columns limit_name, limit_value, effective_date out of json file and read from it continuously
- read to a DataFrame from static table in a database (postgresql in this case)
- capture traffic
- aggregate packets to <k,v> pairs
- manipulate <k,v> pairs stream in order to count traffic only from selected ip addresses
- since reading the data from traffic_limits table is a stream reading it updates once data in table changes
- there is an option to update only once per 20 minutes

This project is still learning how to
- send messages to kafka only when a condition is true
- run unit-tests
