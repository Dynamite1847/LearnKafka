package org.learnKafka.test.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class KafkaProducerIdemTest {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        //Create ConfigMap
        HashMap<String, Object> configMap = new HashMap<>();

        configMap.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        configMap.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configMap.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configMap.put(ProducerConfig.ACKS_CONFIG, "all");
        configMap.put(ProducerConfig.RETRIES_CONFIG, 5);
        configMap.put(ProducerConfig.BATCH_SIZE_CONFIG, 5);
        configMap.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG,3000);
        //Create Producer
        //Need to set <k,V>
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(configMap);


        for (int i = 0; i < 10; i++) {
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(
                    "test","key" + i,"value"+i
            );
            Future<RecordMetadata> send = kafkaProducer.send(producerRecord);
        }
        kafkaProducer.close();
    }
}
