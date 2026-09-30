package io.translab.tantor.server;

import org.apache.kafka.clients.producer.*;
import java.util.*;

public class ProduceMore {
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            throw new IllegalArgumentException("Usage: ProduceMore <bootstrap-servers> <topic> [message-count]");
        }
        String bootstrap = args[0];
        String topic = args[1];
        int messageCount = args.length > 2 ? Integer.parseInt(args[2]) : 246;
        
        Properties prodProps = new Properties();
        prodProps.put("bootstrap.servers", bootstrap);
        prodProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        prodProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        
        try (Producer<String, String> producer = new KafkaProducer<>(prodProps)) {
            for (int i = 0; i < messageCount; i++) {
                producer.send(new ProducerRecord<>(topic, "key", "msg" + i));
            }
            System.out.println("Produced " + messageCount + " messages.");
        }
    }
}
