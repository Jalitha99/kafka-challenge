package org.example;

import org.apache.kafka.clients.producer.*;
import java.util.Properties;
import java.util.Random;

public class Producer_d{

    private static final String TOPIC = "041";
    private static final Random random = new Random();

    public static void main(String[] args) {
//
//        Properties props = new Properties();
//        props.put("bootstrap.servers", "localhost:9092");
//        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        // Optimize producer configuration
//        props.put("acks", "all"); // Ensure message durability
//        props.put("retries", 0);  // Number of retries on message send failure
//        props.put("batch.size", 16384); // Batch size in bytes
//        props.put("linger.ms", 1); // Linger time in milliseconds
//        props.put("buffer.memory", 33554432); // Buffer memory size in bytes
//        props.put("compression.type", "snappy"); // Compression type

        Properties props=new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
// Optimize producer configuration for low latency
        props.put("acks", "0"); // No acknowledgments for lowest latency
        props.put("retries", 3);  // Number of retries on message send failure
        props.put("batch.size", 8192); // Reduced batch size for lower latency
        props.put("linger.ms", 1); // Small linger time
        props.put("buffer.memory", 33554432); // Buffer memory size in bytes
        props.put("compression.type", "snappy"); // Compression type
        props.put("max.in.flight.requests.per.connection", 5); // Higher in-flight requests for steady flow

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        int numMessages = 100000000; // 250 million messages

        long startTime = System.currentTimeMillis();

        try {
            for (int i = 0; i < numMessages; i += 1000) {
                String key = Integer.toString(i % 16);
                StringBuilder valueBuilder = new StringBuilder();
                for (int j = 0; j < 999; j++) {
                    int randomNumber = random.nextInt(1000000) + 1;
                    valueBuilder.append(randomNumber).append(",");
                }
                int randomNumber = random.nextInt(1000000) + 1;
                valueBuilder.append(randomNumber); // Append the last number without trailing comma
                String value = valueBuilder.toString();

                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, key, value);

//                if (i % 100000 == 0) {
//                    System.out.println(i);
//                }

                // Send asynchronously
                producer.send(record, new Callback() {
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if (exception != null) {
                            exception.printStackTrace();
                        }
                    }
                });
            }
        } finally {
            producer.close();
        }

        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;

        System.out.println("Time taken to send " + numMessages + " messages: " + duration + " milliseconds");
    }
}