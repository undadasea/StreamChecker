import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class Producer  extends Thread{
    private final KafkaProducer<Integer, String> producer;
    private final String topic;

    private static final String KAFKA_SERVER_URL = "localhost";
    private static final int KAFKA_SERVER_PORT = 9092;
    private static final String CLIENT_ID = "Producer";

    public Producer(String topic) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", KAFKA_SERVER_URL + ":" + KAFKA_SERVER_PORT);
        properties.put("client.id", CLIENT_ID);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(properties);
        this.topic = topic;
    }

    @SuppressWarnings("InfiniteLoopStatement")
    public void run() {
        int key_value = 1;
        while (true) {
            String message = "message_"+key_value;
            long startTime = System.currentTimeMillis();
            ProducerRecord rec = new ProducerRecord<>(topic, key_value, message);
            producer.send(rec);
            ++key_value;
        }
    }

    public static void main(String[] args) {
        Producer producer = new Producer("topic-name");
        producer.run();
    }
}
