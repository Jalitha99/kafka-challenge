import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class Consumer {

    private static final String TOPIC = "random-numbers";

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "test-group");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(TOPIC));

        int numMessages = 100; // 1 billion messages
        int messageCount = 0;
        long startTime = System.currentTimeMillis();

        try {
            while (messageCount < numMessages) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
//                    System.out.println("Received number: " + record.value());
                    messageCount++;
                    if (messageCount >= numMessages) {
                        break;
                    }
                }
            }
        } finally {
            consumer.close();
        }

        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;

        System.out.println("Time taken to process " + numMessages + " messages: " + duration + " milliseconds");
    }
}
