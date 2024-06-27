import org.apache.kafka.clients.producer.*;
import java.util.Properties;
import java.util.Random;

public class Producer {

    private static final String TOPIC = "random-numbers";
    private static final Random random = new Random();

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        int numMessages = 10000000; // 10 million messages

        long startTime = System.currentTimeMillis();

        try {
            for (int i = 0; i < numMessages; i++) {
                int randomNumber = random.nextInt(1000000) + 1;
                String key = "Number";
                String value = Integer.toString(randomNumber);
                producer.send(new ProducerRecord<>(TOPIC, key, value));
            }
        } finally {
            producer.close();
        }

        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;

        System.out.println("Time taken to send " + numMessages + " messages: " + duration + " milliseconds");
    }
}
