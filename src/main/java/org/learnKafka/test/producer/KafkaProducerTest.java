package org.learnKafka.test.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;

public class KafkaProducerTest {
    public static void main(String[] args) {
        //Create ConfigMap
        HashMap<String, Object> configMap = new HashMap<>();

        configMap.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        configMap.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configMap.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Create Producer
        //Need to set <k,V>
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(configMap);

        //Create Data

        for (int i = 0; i < 10; i++) {
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(
                    "test","key" + i,"value"+i
            );
            //Send to Kafka by Producer
            kafkaProducer.send(producerRecord);
        }
        kafkaProducer.close();
    }
}