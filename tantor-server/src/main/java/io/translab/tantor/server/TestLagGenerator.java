package io.translab.tantor.server;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import java.util.*;

public class TestLagGenerator {
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            throw new IllegalArgumentException("Usage: TestLagGenerator <bootstrap-servers> <topic>");
        }
        String bootstrap = args[0];
        String topic = args[1];

        // 1. Produce 100 messages
        Properties prodProps = new Properties();
        prodProps.put("bootstrap.servers", bootstrap);
        prodProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        prodProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        
        try (Producer<String, String> producer = new KafkaProducer<>(prodProps)) {
            for (int i = 0; i < 100; i++) {
                producer.send(new ProducerRecord<>(topic, "key", "msg" + i));
            }
            System.out.println("Produced 100 messages.");
        }

        // 2. Start Consumer Group 1 (Reads 50 msgs -> Lag 50)
        Properties consProps1 = new Properties();
        consProps1.put("bootstrap.servers", bootstrap);
        consProps1.put("group.id", "group-lag-50");
        consProps1.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consProps1.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consProps1.put("auto.offset.reset", "earliest");
        
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consProps1)) {
            consumer.subscribe(Collections.singletonList(topic));
            int read = 0;
            while (read < 50) {
                ConsumerRecords<String, String> records = consumer.poll(java.time.Duration.ofMillis(500));
                for (ConsumerRecord<String, String> r : records) read++;
                consumer.commitSync();
            }
            System.out.println("Group 1 read " + read + " messages. Lag should be 50.");
        }
        
        // 3. Start Consumer Group 2 (Reads 90 msgs -> Lag 10)
        Properties consProps2 = new Properties();
        consProps2.put("bootstrap.servers", bootstrap);
        consProps2.put("group.id", "group-lag-10");
        consProps2.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consProps2.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consProps2.put("auto.offset.reset", "earliest");
        
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consProps2)) {
            consumer.subscribe(Collections.singletonList(topic));
            int read = 0;
            while (read < 90) {
                ConsumerRecords<String, String> records = consumer.poll(java.time.Duration.ofMillis(500));
                for (ConsumerRecord<String, String> r : records) read++;
                consumer.commitSync();
            }
            System.out.println("Group 2 read " + read + " messages. Lag should be 10.");
        }
        
        // 4. Start Consumer Group 3 (Reads 0 msgs -> Lag 100)
        Properties consProps3 = new Properties();
        consProps3.put("bootstrap.servers", bootstrap);
        consProps3.put("group.id", "group-lag-100");
        consProps3.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consProps3.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consProps3.put("auto.offset.reset", "earliest");
        
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consProps3)) {
            consumer.subscribe(Collections.singletonList(topic));
            // Read 0 to just register the group
            consumer.poll(java.time.Duration.ofMillis(100));
            consumer.commitSync(); // Force commit offset 0
            System.out.println("Group 3 registered with 0 messages read. Lag should be 100.");
        }
    }
}
