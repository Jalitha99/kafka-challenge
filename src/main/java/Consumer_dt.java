import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class Consumer_dt {
    public static final String TOPIC = "a027"; // Match the topic name from your producer
    private static final String GROUP_ID = "consumer-group1";
    private static final String OUTPUT_FILE = "counts.csv";
    public static final int POLL_TIMEOUT = 10000; // Poll timeout in milliseconds
    private static final int NUM_THREADS = 4; // Number of consumer threads

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.31.10.129:9092, 172.31.10.129:9093, 172.31.10.129:9094, 172.31.10.129:9095"); // Match your Kafka server address
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        ExecutorService executorService = Executors.newFixedThreadPool(NUM_THREADS);
        Map<Integer, Integer> counts = Collections.synchronizedMap(new HashMap<>());
        AtomicInteger totalCount = new AtomicInteger(0); // Shared total count across all threads

        long startTime = System.currentTimeMillis();

        for (int i = 0; i < NUM_THREADS; i++) {
            executorService.execute(new ConsumerWorker(props, counts, totalCount));
        }

        executorService.shutdown();
        while (!executorService.isTerminated()) {
            // Wait for all threads to finish
        }

        long endTime = System.currentTimeMillis();
        long elapsedTime = endTime - startTime;
        saveCountsToCsv(counts);
        System.out.println("Total messages consumed: " + totalCount.get());
        System.out.println("Total time taken: " + elapsedTime + " milliseconds");
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

class ConsumerWorker implements Runnable {
    private final Properties props;
    private final Map<Integer, Integer> counts;
    private final AtomicInteger totalCount; // Shared thread-safe counter for total message count
    public static final int POLL_TIMEOUT = 10000; // Poll timeout in milliseconds

    ConsumerWorker(Properties props, Map<Integer, Integer> counts, AtomicInteger totalCount) {
        this.props = props;
        this.counts = counts;
        this.totalCount = totalCount; // Assign the shared total count instance
    }

    @Override
    public void run() {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(Consumer_dt.TOPIC));

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(POLL_TIMEOUT);
                if (records.isEmpty()) {
                    break;
                }

                for (ConsumerRecord<String, String> record : records) {
                    int number = Integer.parseInt(record.value());
                    synchronized (counts) {
                        counts.put(number, counts.getOrDefault(number, 0) + 1);
                    }
                    totalCount.incrementAndGet(); // Increment total count atomically
                    if (totalCount.get() % 1000000 == 0){
                        System.out.println("Message Consumed :" + totalCount.get());
                    }
//                    System.out.println("Partition: " + record.partition() + ", Offset: " + record.offset() + ", Value: " + record.value());
                }
            }
        } finally {
            consumer.close();
        }
    }
}
