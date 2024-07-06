package org.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Consumer_dt_2 {
    public static final String TOPIC = "028"; // Match the topic name from your producer
    private static final String GROUP_ID = "consumer-group1";
    private static final String SERVER_IP = "172.15.10.106"; // Aggregator address
    private static final int SERVER_PORT = 12345;
    private static final String OUTPUT_FILE = "counts.csv";
    public static final int POLL_TIMEOUT = 10000; // Poll timeout in milliseconds
    private static final int NUM_THREADS = 4; // Number of consumer threads

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"); // Match your Kafka server address
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        ExecutorService executorService = Executors.newFixedThreadPool(NUM_THREADS);
        int[] counts_1 = new int[1000001]; // Array to count occurrences, from 1 to 1,000,000 (index 0 is unused)
        int[] counts_2 = new int[1000001]; // Array to count occurrences, from 1 to 1,000,000 (index 0 is unused)
        int[] counts_3 = new int[1000001]; // Array to count occurrences, from 1 to 1,000,000 (index 0 is unused)
        int[] counts_4 = new int[1000001]; // Array to count occurrences, from 1 to 1,000,000 (index 0 is unused)
        int[][] num_of_counts = {counts_1, counts_2, counts_3, counts_4};
        int[] total_count = new int[1000001];

        long startTime = System.currentTimeMillis();

        for (int i = 0; i < NUM_THREADS; i++) {
            executorService.execute(new ConsumerWorker(props, num_of_counts[i]));
        }

        executorService.shutdown();
        while (!executorService.isTerminated()) {
            // Wait for all threads to finish
        }


//        for (int i = 0; i < counts_1.length; i++) {
//            total_count[i] = counts_1[i] + counts_2[i] + counts_3[i] + counts_4[i];
//        }

        for (int i = 0; i < counts_1.length; i++) {
            for (int j = 0; j < num_of_counts.length; j++) {
                total_count[i] += num_of_counts[j][i];
            }
        }

        sendCountsToServer(total_count);

        long endTime = System.currentTimeMillis();
        long elapsedTime = endTime - startTime;
//        saveCountsToCsv(counts);
        System.out.println("Total time taken: " + elapsedTime + " milliseconds");
    }

    private static void sendCountsToServer(int[] counts) {
        try (Socket socket = new Socket(SERVER_IP, SERVER_PORT);
             ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream())) {
            out.writeObject(counts);
            out.flush();
            System.out.println("Counts sent to server at " + SERVER_IP + ":" + SERVER_PORT);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

class ConsumerWorker implements Runnable {
    private final Properties props;
    private final int[] counts;
    public static final int POLL_TIMEOUT = 10000; // Poll timeout in milliseconds

    ConsumerWorker(Properties props, int[] counts) {
        this.props = props;
        this.counts = counts;
    }

    @Override
    public void run() {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(Consumer_dt_2.TOPIC));

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(POLL_TIMEOUT);
                if (records.isEmpty()) {
                    break;
                }

                for (ConsumerRecord<String, String> record : records) {
                    String[] numbers = record.value().split(",");
                    for (String numberStr : numbers) {
                        int number = Integer.parseInt(numberStr.trim());
                        if (number >= 1 && number <= 1000000) { // Ensure the number is within the expected range
                            counts[number]++;
                        }
                    }
                }
            }
        } finally {
            consumer.close();
        }
    }
}