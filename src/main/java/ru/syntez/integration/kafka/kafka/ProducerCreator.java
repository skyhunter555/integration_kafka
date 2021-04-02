package ru.syntez.integration.kafka.kafka;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongSerializer;
import ru.syntez.integration.kafka.entities.RoutingDocument;
import ru.syntez.integration.kafka.utils.RoutingDocumentSerializer;

public class ProducerCreator {

    public static Producer<Long, RoutingDocument> createProducer(KafkaConfig kafkaConfig) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfig.getBrokers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, RoutingDocumentSerializer.class.getName());

        //props.put(ProducerConfig.CLIENT_ID_CONFIG, kafkaConfig.getProducer().getClientId());
        props.put(ProducerConfig.ACKS_CONFIG, kafkaConfig.getProducer().getAcks());
        props.put(ProducerConfig.RETRIES_CONFIG, kafkaConfig.getProducer().getRetries());
        props.put(ProducerConfig.LINGER_MS_CONFIG, kafkaConfig.getProducer().getLingerMs());
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, kafkaConfig.getProducer().getRequestTimeoutMs());
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, kafkaConfig.getProducer().getDeliveryTimeoutMs());

        //props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CustomPartitioner.class.getName());
        return new KafkaProducer<>(props);
    }

}
