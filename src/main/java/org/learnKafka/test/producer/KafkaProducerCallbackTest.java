package org.learnKafka.test.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class KafkaProducerCallbackTest {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
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
            //异步发送
            Future<RecordMetadata> send = kafkaProducer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    System.out.println("数据发送成功： " + metadata);
                }
            });
            //同步操作时阻塞线程，等到成功才发送下一条
            send.get();
        }
        kafkaProducer.close();
    }
}
