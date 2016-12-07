import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;

import java.util.Arrays;
import java.util.Properties;

public class KafkaConsumer {

    private static Consumer<String, String> consumer;
    private static final String topic = "mytopic";

    public void init() {
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "1");
        consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, "simple");
        consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<String, String>(consumerProps);
    }

    public void consume() {
        consumer.subscribe(Arrays.asList(topic));
        // start processing messages
        try {
            while (true) {
                // poll kafka server every 100 ms
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(record.value());
                }
            }
        } catch (WakeupException ex) {
            System.out.println("Exception caught: " + ex.getMessage());
        } finally {
            consumer.close();
            System.out.println("KafkaConsumer was closed.");
        }
    }

    public static void main(String[] args) {
        KafkaConsumer kafkaConsumer = new KafkaConsumer();
        kafkaConsumer.init();
        kafkaConsumer.consume();
    }
}
