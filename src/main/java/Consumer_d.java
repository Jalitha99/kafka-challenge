import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Consumer_d {
    private static final String TOPIC = "a019";
    private static final String GROUP_ID = "consumer-group1";
    private static final String OUTPUT_FILE = "counts.csv";
    private static final int POLL_TIMEOUT = 10000;
    private static final int NUM_CONSUMER_THREADS = 4; // Number of consumer threads

    public static void main(String[] args) {
        ExecutorService executor = Executors.newFixedThreadPool(NUM_CONSUMER_THREADS);

        for (int i = 0; i < NUM_CONSUMER_THREADS; i++) {
            executor.submit(new ConsumerTask());
        }

        // Shutdown hook to ensure the executor is properly shut down
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(60, java.util.concurrent.TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
            }
        }));
    }

    static class ConsumerTask implements Runnable {
        private final KafkaConsumer<String, String> consumer;
        private final Map<Integer, Integer> counts = new HashMap<>();
        private int totalMessagesConsumed = 0;

        ConsumerTask() {
            Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.31.10.129:9092");
            props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            consumer = new KafkaConsumer<>(props);
            consumer.subscribe(Collections.singletonList(TOPIC));
        }

        @Override
        public void run() {
            long startTime = System.currentTimeMillis();

            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(POLL_TIMEOUT);
                    if (records.isEmpty()) {
                        break;
                    }

                    for (ConsumerRecord<String, String> record : records) {
                        int number = Integer.parseInt(record.value());
                        if (totalMessagesConsumed % 100000 == 0) {
                            System.out.println("Thread " + Thread.currentThread().getId() + " - Number: " + totalMessagesConsumed);
                        }
                        synchronized (counts) {
                            counts.put(number, counts.getOrDefault(number, 0) + 1);
                        }
                        totalMessagesConsumed++;
                    }
                }
            } finally {
                consumer.close();
            }

            saveCountsToCsv(counts);

            long endTime = System.currentTimeMillis();
            System.out.println("Thread " + Thread.currentThread().getId() + " - Consumed " + totalMessagesConsumed + " messages and saved counts in " + (endTime - startTime) / 1000 + " seconds");
        }

        private void saveCountsToCsv(Map<Integer, Integer> counts) {
            try (FileWriter writer = new FileWriter(OUTPUT_FILE, true)) {
                synchronized (counts) {
                    for (Map.Entry<Integer, Integer> entry : counts.entrySet()) {
                        writer.write(entry.getKey() + "," + entry.getValue() + "\n");
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
