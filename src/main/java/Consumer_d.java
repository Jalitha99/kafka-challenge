import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class Consumer_d{
    private static final String TOPIC = "random-numbers_topic"; // Match the topic name from your producer
    private static final String GROUP_ID = "consumer_group";
    private static final String OUTPUT_FILE = "counts.csv";
    private static final int POLL_TIMEOUT = 1000;
    private static final int EXPECTED_MESSAGE_COUNT = 1000000000; // 1 billion messages

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"); // Match your Kafka server address
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(TOPIC));

        Map<Integer, Integer> counts = new HashMap<>();
        int totalMessagesConsumed = 0;

        long startTime = System.currentTimeMillis();

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(POLL_TIMEOUT);
                if (records.isEmpty()) {
                    if (totalMessagesConsumed >= EXPECTED_MESSAGE_COUNT) {
                        break;
                    }
                    continue;
                }
                for (ConsumerRecord<String, String> record : records) {
                    int number = Integer.parseInt(record.value());
                    counts.put(number, counts.getOrDefault(number, 0) + 1);
                    totalMessagesConsumed++;
                }
            }
        } finally {
            consumer.close();
        }

        saveCountsToCsv(counts);

        long endTime = System.currentTimeMillis();
        System.out.println("Consumed " + totalMessagesConsumed + " messages and saved counts in " + (endTime - startTime) / 1000 + " seconds");
    }

    private static void saveCountsToCsv(Map<Integer, Integer> counts) {
        try (FileWriter writer = new FileWriter(OUTPUT_FILE)) {
            for (Map.Entry<Integer, Integer> entry : counts.entrySet()) {
                writer.write(entry.getKey() + "," + entry.getValue() + "\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
