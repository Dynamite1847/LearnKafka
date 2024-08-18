package org.learnKafka.test.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Set;

public class KafkaConsumerAutoOffsetTest {
    public static void main(String[] args) {

        HashMap<String, Object> consumerConfig = new HashMap<>();
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerConfig.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        consumerConfig.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
//        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "test1");

        //Create Consumer
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(consumerConfig);

        //subscribe topic
        kafkaConsumer.subscribe(Collections.singletonList("test"));

        boolean flag = true;
        while (flag) {
             kafkaConsumer.poll(Duration.ofMillis(100));
            Set<TopicPartition> assignment = kafkaConsumer.assignment();
            if (assignment != null && !assignment.isEmpty()) {
                for (TopicPartition topicPartition : assignment) {
                    if (topicPartition.topic().equals("test")) {
                        kafkaConsumer.seek(topicPartition, 2L);
                        flag = false;
                    }
                }
            }
        }
        //get data from Kafka topic
        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record);
            }

            //Save offset manually
            kafkaConsumer.commitSync();//同步提交，会阻塞线程，直到提交成功，会对性能产生影响
            kafkaConsumer.commitAsync();//异步提交
        }

        //LEO： Log End Offset = size + 1 = 0,1,2,3
    }
}
