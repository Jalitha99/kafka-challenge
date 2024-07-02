import org.apache.kafka.clients.producer.*;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Producer_compression_added {
    public static final String TOPIC = "test4";
    private static final int NUM_MESSAGES = 100000000; // Adjust as needed
    private static final int NUM_THREADS = 4; // Number of producer threads

    public static void main(String[] args) {
        Properties props = new Properties();
//        props.put("bootstrap.servers", "172.31.10.129:9092,broker1:9092,broker2:9092,broker3:9092");
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // Optimize producer configuration
        props.put("acks", "all"); // Ensure message durability
        props.put("retries", 0);  // Number of retries on message send failure
        props.put("batch.size", 131072); // Batch size in bytes
        props.put("linger.ms", 10); // Linger time in milliseconds
        props.put("buffer.memory", 33554432); // Buffer memory size in bytes
        props.put("compression.type", "snappy"); // Compression type for batched messages


        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        ExecutorService executorService = Executors.newFixedThreadPool(NUM_THREADS);

        long startTime = System.currentTimeMillis();

        for (int i = 0; i < NUM_THREADS; i++) {
            executorService.execute(new ProducerWorker1(producer, NUM_MESSAGES / NUM_THREADS));
        }

        executorService.shutdown();
        while (!executorService.isTerminated()) {
            // Wait for all threads to finish
        }

        producer.close();

        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;

        System.out.println("Time taken to send " + NUM_MESSAGES + " messages: " + duration + " milliseconds");
    }
}

class ProducerWorker1 implements Runnable {
    private final KafkaProducer<String, String> producer;
    private final int numMessages;
    private static final Random random = new Random();

    ProducerWorker1(KafkaProducer<String, String> producer, int numMessages) {
        this.producer = producer;
        this.numMessages = numMessages;
    }

    @Override
    public void run() {
        for (int i = 0; i < numMessages; i++) {
            int randomNumber = random.nextInt(1000000) + 1;
            String key = Integer.toString(i % 16); // Use modulus to distribute keys evenly
            String value = Integer.toString(randomNumber);
            ProducerRecord<String, String> record = new ProducerRecord<>(Producer_compression_added.TOPIC, key, value);

            if (i % 1000000 == 0){
                System.out.println(i/1000000 + "    Thread ID: " +Thread.currentThread().getId());
            }

            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception != null) {
                        exception.printStackTrace();
                    }
//                    else {
//                        System.out.println("Sent message to partition: " + metadata.partition() + ", offset: " + metadata.offset() + ", thread id: " + Thread.currentThread().getId());
//                    }
                }
            });
        }
    }
}



