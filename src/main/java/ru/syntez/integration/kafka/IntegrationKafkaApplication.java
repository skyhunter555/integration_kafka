package ru.syntez.integration.kafka;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.JacksonXmlModule;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.yaml.snakeyaml.Yaml;
import ru.syntez.integration.kafka.entities.RoutingDocument;
import ru.syntez.integration.kafka.kafka.ConsumerCreator;
import ru.syntez.integration.kafka.kafka.KafkaConfig;
import ru.syntez.integration.kafka.kafka.ProducerCreator;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Main class
 *
 * @author Skyhunter
 * @date 06.04.2021
 */
public class IntegrationKafkaApplication {

    private final static Logger LOG = Logger.getLogger(IntegrationKafkaApplication.class.getName());
    private static AtomicInteger msg_sent_counter = new AtomicInteger(0);
    private static AtomicInteger msg_received_counter = new AtomicInteger(0);
    private static KafkaConfig config;
    private static Map<String, Set<String>> consumerRecordSetMap = new ConcurrentHashMap<>();

    private static ObjectMapper xmlMapper() {
        JacksonXmlModule xmlModule = new JacksonXmlModule();
        xmlModule.setDefaultUseWrapper(false);
        ObjectMapper xmlMapper = new XmlMapper(xmlModule);
        xmlMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        return new XmlMapper(xmlModule);
    }

    public static void main(String[] args) {

        Yaml yaml = new Yaml();
        //try( InputStream in = Files.newInputStream( Paths.get( args[ 0 ] ) ) ) {
        try( InputStream in = Files.newInputStream(Paths.get(IntegrationKafkaApplication.class.getResource("/application.yml").toURI()))) {
            config = yaml.loadAs( in, KafkaConfig.class );
            LOG.log(Level.INFO, config.toString());
        } catch (URISyntaxException | IOException e) {
            LOG.log(Level.WARNING, "Error load KafkaConfig from resource", e);
            return;
        }

        try {
            runConsumers( 3); //Для 1-го и 2-го кейса = 3, для 3-го кейса = 2
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        //Result
        LOG.info(String.format("Количество отправленных уникальных сообщений: %s", msg_sent_counter.get()));
        LOG.info(String.format("Количество всех принятых сообщений: %s", msg_received_counter.get()));
        Integer uniqueCount = 0;
        for (Map.Entry entry: consumerRecordSetMap.entrySet()) {
            Set consumerRecordSet = (Set) entry.getValue();
            uniqueCount = uniqueCount + consumerRecordSet.size();
            BigDecimal percent = (BigDecimal.valueOf(100).multiply(BigDecimal.valueOf(consumerRecordSet.size())))
                    .divide(BigDecimal.valueOf(msg_received_counter.longValue()), 3);
            LOG.info(String.format("Количество принятых уникальных сообщений на консюмере %s: %s (%s)",
                    entry.getKey(),
                    consumerRecordSet.size(),
                    percent
            ));
        }
        LOG.info(String.format("Количество принятых уникальных сообщений: %s", uniqueCount));

    }

    private static void startConsumer(String consumerId, String consumerGroup) {
        Consumer consumer = ConsumerCreator.createConsumer(config, consumerGroup);
        while (msg_received_counter.get() < config.getMessageCount()) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> record : records) {
                msg_received_counter.incrementAndGet();
                Set consumerRecordSet = consumerRecordSetMap.get(consumerId);
                if (consumerRecordSet == null) {
                    consumerRecordSet = new HashSet();
                }
                consumerRecordSet.add(record.value());
                consumerRecordSetMap.put(consumerId, consumerRecordSet);
                LOG.info(String.format("Consumer %s read record number=%s, value=%s, partition=%s, offset = %s",
                        consumerId,
                        consumerRecordSet.size(),
                        record.value(),
                        record.partition(),
                        record.offset()
                ));
            }
            consumer.commitSync();
        }
    }

    private static void runConsumers(Integer consumerCount) throws InterruptedException {

        ExecutorService executorService = Executors.newFixedThreadPool(consumerCount + 1);
        for (int i = 0; i < consumerCount; i++) {
            String consumerId = Integer.toString(i + 1);
            executorService.execute(() -> startConsumer(consumerId, config.getGroupIdConfig()));
        }
        executorService.execute(IntegrationKafkaApplication::runProducer);
        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.MINUTES);
    }

    private static void runProducer() {
        String messageXml;
        RoutingDocument document;
        try {
            messageXml = new String(Files.readAllBytes(Paths.get(IntegrationKafkaApplication.class.getResource("/router_doc_1.xml").toURI())));
            document = xmlMapper().readValue(messageXml, RoutingDocument.class);
        } catch (IOException | URISyntaxException e) {
            LOG.log(Level.WARNING, "Error readValue from resource", e);
            return;
        }
        Producer<Long, RoutingDocument> producer = ProducerCreator.createProducer(config);
        for (int index = 0; index < config.getMessageCount(); index++) {
            document.setDocId(index);
            ProducerRecord<Long, RoutingDocument> record = new ProducerRecord<>(config.getTopicName(), document);
            try {
                RecordMetadata metadata = producer.send(record).get();
                LOG.info(String.format("Record %s sent to partition=%s with offset=%s", index, metadata.partition(), metadata.offset()));
                msg_sent_counter.incrementAndGet();
            } catch (ExecutionException | InterruptedException e) {
                LOG.log(Level.WARNING, "Error in sending record", e);
            }
        }

    }
}
