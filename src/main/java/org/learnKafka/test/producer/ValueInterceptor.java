package org.learnKafka.test.producer;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * 自定义拦截器
 * 1. 实现ProducerInterceptor
 * 2. 定义泛型
 * 3. 重写方法
 */
public class ValueInterceptor implements ProducerInterceptor<String, String> {
    /**
     *
     * @param record the record from client or the record returned by the previous interceptor in the chain of interceptors.
     *
     */
    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        return new ProducerRecord<>(record.topic(), record.key(), record.value()+record.value());
    }

    /**
     *
     * @param metadata The metadata for the record that was sent (i.e. the partition and offset).
     *                 If an error occurred, metadata will contain only valid topic and maybe
     *                 partition. If partition is not given in ProducerRecord and an error occurs
     *                 before partition gets assigned, then partition will be set to RecordMetadata.NO_PARTITION.
     *                 The metadata may be null if the client passed null record to
     *                 {@link org.apache.kafka.clients.producer.KafkaProducer#send(ProducerRecord)}.
     * @param exception The exception thrown during processing of this record. Null if no error occurred.
     */
    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {

    }

    /**
     * 生产者对象关闭时调用此方法
     */
    @Override
    public void close() {

    }

    /**
     *
     * @param configs
     */
    @Override
    public void configure(Map<String, ?> configs) {

    }
}
