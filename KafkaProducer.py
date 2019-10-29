import time
from time import gmtime, strftime
from kafka import KafkaProducer

import socket
import struct

# this module captures traffic on all interfaces and sends information
# about packets to the kafka topic

# runs with python KafkaProducer.py (after all packages installed)

def get_str_bytes_ip(ip):
    addr_array = ip.split(".")
    for i in range(4):
        addr_array[i] = "{0:#0{1}x}".format(int(addr_array[i]),4)

    return "\\"+addr_array[0]+"\\"+ \
                addr_array[1]+"\\"+ \
                addr_array[2]+"\\"+ \
                addr_array[3]

def publish_message(producer_instance, topic_name, key, value):
    try:
        key_bytes = bytes(key, encoding='utf-8')
        value_bytes = bytes(value, encoding='utf-8')
        print("key_bytes: ", key_bytes)
        producer_instance.send(topic_name, key=key_bytes, value=value_bytes)
        producer_instance.flush()
        print("Message '{0}' published successfully.".format(value))
    except Exception as ex:
        print('Exception in publishing message')
        print(str(ex))

def connect_kafka_producer():
    _producer = None
    try:
        _producer = KafkaProducer(bootstrap_servers=['localhost:9092'], api_version=(0, 10))
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return _producer

def main(sleep_sec=10):
    kafka_producer = connect_kafka_producer()

    Socket=socket.socket(socket.AF_INET, socket.SOCK_RAW, socket.IPPROTO_TCP)
    while True:
      packet = Socket.recvfrom(65565)
      ip_header = struct.unpack("!BBHHHBBH4s4s", packet[0][0:20])
      source_ip = socket.inet_ntoa(ip_header[8])
      destination_ip = socket.inet_ntoa(ip_header[9])
      total_length = ip_header[2]
      publish_message(kafka_producer, 'new_topic', get_str_bytes_ip(source_ip)+ \
            get_str_bytes_ip(destination_ip), str(total_length))
      time.sleep(sleep_sec)


if __name__ == '__main__':
    main(sleep_sec=1)
