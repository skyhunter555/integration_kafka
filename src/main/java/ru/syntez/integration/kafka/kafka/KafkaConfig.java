package ru.syntez.integration.kafka.kafka;

import lombok.Data;

@Data
public class KafkaConfig {

    private String  brokers;
    private Integer messageCount;
    private String  topicName;
    private String  groupIdConfig;
    private Integer maxNoMessageFoundCount;
    private ConsumerConfig consumer;
    private ProducerConfig producer;
}
