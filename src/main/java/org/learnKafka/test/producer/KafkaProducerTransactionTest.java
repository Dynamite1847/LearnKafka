package org.learnKafka.test.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class KafkaProducerTransactionTest {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        //Create ConfigMap
        HashMap<String, Object> configMap = new HashMap<>();

        configMap.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        configMap.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configMap.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configMap.put(ProducerConfig.ACKS_CONFIG, "all");
        configMap.put(ProducerConfig.RETRIES_CONFIG, 5);
        configMap.put(ProducerConfig.BATCH_SIZE_CONFIG, 5);
        configMap.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 3000);

        //事务是基于幂等性操作的
        configMap.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "mytransactionid");

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(configMap);

        //init transaction
        kafkaProducer.initTransactions();

        try {
            //start transaction
            kafkaProducer.beginTransaction();
            for (int i = 0; i < 10; i++) {
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(
                        "test", "key" + i, "value" + i
                );
                Future<RecordMetadata> send = kafkaProducer.send(producerRecord);
            }
            //commit transaction
            kafkaProducer.commitTransaction();
        } catch (Exception e) {
            e.printStackTrace();
            //abort transaction
            kafkaProducer.abortTransaction();
        } finally {
            kafkaProducer.close();
        }
    }
}
