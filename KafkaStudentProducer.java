import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.Future;

public class KafkaStudentProducer {
    private final KafkaProducer<String, String> producer;
    private final String topic;

    public KafkaStudentProducer(String bootstrapServers, String topic) {
        this.topic = topic;
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        producer = new KafkaProducer<>(props);
    }

    public void sendStudentDetails(String jsonStudent) {
        try {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, jsonStudent);
            Future<RecordMetadata> future = producer.send(record);
            RecordMetadata metadata = future.get();
            System.out.printf("Message sent to topic:%s partition:%d offset:%d%n",
                    metadata.topic(), metadata.partition(), metadata.offset());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void close() {
        producer.close();
    }

    public static void main(String[] args) {
        // Example JSON representing a student
        String studentJson = "{ \"rollNo\": \"101\", \"firstName\": \"John\", \"lastName\": \"Doe\" }";
        String bootstrapServers = "localhost:9092"; // adjust if needed
        String topic = "student-topic";

        KafkaStudentProducer producer = new KafkaStudentProducer(bootstrapServers, topic);
        producer.sendStudentDetails(studentJson);
        producer.close();
    }
}
