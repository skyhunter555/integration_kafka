package ru.syntez.integration.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.JacksonXmlModule;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.config.LogLevelConfig;
import ru.syntez.integration.kafka.entities.RoutingDocument;
import ru.syntez.integration.kafka.kafka.ConsumerCreator;
import ru.syntez.integration.kafka.kafka.IKafkaConstants;
import ru.syntez.integration.kafka.kafka.ProducerCreator;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Main class
 *
 * @author Skyhunter
 * @date 01.04.2021
 */
public class IntergationKafkaApplication {

    private final static Logger LOG = Logger.getLogger(IntergationKafkaApplication.class.getName());

    private static ObjectMapper xmlMapper() {
        JacksonXmlModule xmlModule = new JacksonXmlModule();
        xmlModule.setDefaultUseWrapper(false);
        ObjectMapper xmlMapper = new XmlMapper(xmlModule);
        xmlMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        return new XmlMapper(xmlModule);
    }

    public static void main(String[] args) {
        runProducer();
        // runConsumer();
    }

    static void runConsumer() {

        Consumer<Long, String> consumer = ConsumerCreator.createConsumer();

        int noMessageFound = 0;

        while (true) {
            ConsumerRecords<Long, String> consumerRecords = consumer.poll(Duration.ofSeconds(1000));
            // 1000 is the time in milliseconds consumer will wait if no record is found at broker.
            if (consumerRecords.count() == 0) {
                noMessageFound++;
                if (noMessageFound > IKafkaConstants.MAX_NO_MESSAGE_FOUND_COUNT)
                    // If no message found count is reached to threshold exit loop.
                    break;
                else
                    continue;
            }

            //print each record.
            consumerRecords.forEach(record -> {
                LOG.info("Record Key " + record.key());
                LOG.info("Record value " + record.value());
                LOG.info("Record partition " + record.partition());
                LOG.info("Record offset " + record.offset());
            });

            // commits the offset of record to broker.
            consumer.commitAsync();
        }
        consumer.close();
    }

    private static void runProducer() {

        String messageXml;
        RoutingDocument document;
        try {
            messageXml = new String(Files.readAllBytes(Paths.get(IntergationKafkaApplication.class.getResource("/router_doc_1.xml").toURI())));
            document = xmlMapper().readValue(messageXml, RoutingDocument.class);
        } catch (IOException | URISyntaxException e) {
            LOG.log(Level.WARNING, "Error readValue from resource", e);
            return;
        }

        Producer<Long, RoutingDocument> producer = ProducerCreator.createProducer();

        for (int index = 0; index < IKafkaConstants.MESSAGE_COUNT; index++) {
            ProducerRecord<Long, RoutingDocument> record = new ProducerRecord<>(IKafkaConstants.TOPIC_NAME, document);
            try {
                RecordMetadata metadata = producer.send(record).get();
                LOG.info("Record sent with key " + index + " to partition " + metadata.partition()
                        + " with offset " + metadata.offset());
            } catch (ExecutionException | InterruptedException e) {
                LOG.log(Level.WARNING, "Error in sending record", e);
            }
        }
    }
}
