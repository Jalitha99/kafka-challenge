import org.apache.kafka.clients.producer.*;
import java.util.Properties;
import java.util.Random;

public class Producer_d{

    private static final String TOPIC = "random-numbers-topic00";
    private static final Random random = new Random();

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // Optimize producer configuration
        props.put("acks", "all"); // Ensure message durability
        props.put("retries", 0);  // Number of retries on message send failure
        props.put("batch.size", 16384); // Batch size in bytes
        props.put("linger.ms", 1); // Linger time in milliseconds
        props.put("buffer.memory", 33554432); // Buffer memory size in bytes
        //props.put("compression.type", "snappy"); // Remove compression for now

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        int numMessages = 1000000; // 1 billion messages

        long startTime = System.currentTimeMillis();

        try {
            for (int i = 0; i < numMessages; i++) {
                int randomNumber = random.nextInt(1000000) + 1;
                String key = "Number";
                String value = Integer.toString(randomNumber);
                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, key, value);

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
