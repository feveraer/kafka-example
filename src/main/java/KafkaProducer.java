import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.util.Properties;
import java.util.Scanner;

public class KafkaProducer {

    private static Producer<String, String> producer;
    private static final String topic = "mytopic";
    private static Scanner scanner;

    public void init() {
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        producer = new org.apache.kafka.clients.producer.KafkaProducer<String, String>(producerProps);
    }

    public void publishMessage() throws IOException {
        scanner = new Scanner(System.in);
        String message = "";
        do {
            System.out.println("Enter message to send to kafka broker (Type 'exit' to close producer): ");
            // read message from console
            message = scanner.nextLine();
            // publish message as record to topic
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, message);
            producer.send(record);
        } while (!message.equals("exit"));
        scanner.close();
        producer.close();
    }

    public static void main(String[] args) throws IOException {
        KafkaProducer kafkaProducer = new KafkaProducer();
        kafkaProducer.init();
        kafkaProducer.publishMessage();
    }
}
