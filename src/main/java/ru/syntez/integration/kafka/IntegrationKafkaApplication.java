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
import ru.syntez.integration.kafka.entities.DocumentTypeEnum;
import ru.syntez.integration.kafka.entities.RoutingDocument;
import ru.syntez.integration.kafka.exceptions.TestMessageException;
import ru.syntez.integration.kafka.kafka.ConsumerCreator;
import ru.syntez.integration.kafka.kafka.KafkaConfig;
import ru.syntez.integration.kafka.kafka.ProducerCreator;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
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
        try( InputStream in = Files.newInputStream( Paths.get( args[ 0 ] ) ) ) {
            //try( InputStream in = Files.newInputStream(Paths.get(IntegrationKafkaApplication.class.getResource("/application.yml").toURI()))) {
            config = yaml.loadAs( in, KafkaConfig.class );
            LOG.log(Level.INFO, config.toString());
        } catch (IOException e) {
            LOG.log(Level.WARNING, "Error load KafkaConfig from resource", e);
            return;
        }

        try {
            //runConsumers( 3); // 1, 2 case
            //runConsumersWithKeysAndTimeout(3,3);
            runConsumersWithVersionsAndTimeout(3, 3); // 3 case
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        //Result
        LOG.info(String.format("Количество отправленных уникальных сообщений: %s", msg_sent_counter.get()));
        LOG.info(String.format("Количество всех принятых сообщений: %s", msg_received_counter.get()));
        Integer uniqueCount = 0;
        for (Map.Entry entry: consumerRecordSetMap.entrySet()) {
            Set consumerRecordSet = (Set) entry.getValue();
            //for (Object document: consumerRecordSet.toArray()) {
            //    RoutingDocument doc = (RoutingDocument) document;
            //    LOG.info(entry.getKey() + ";" + doc.getDocId() + ";" + doc.getDocType().name());
            //}
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
                LOG.info(String.format("Consumer %s read record key=%s, number=%s, value=%s, partition=%s, offset = %s",
                        consumerId,
                        record.key(),
                        consumerRecordSet.size(),
                        record.value(),
                        record.partition(),
                        record.offset()
                ));
            }
            consumer.commitSync();
        }
    }

    /**
     * For 1 and 2 cases
     */
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

    /**
     * For 3 case
     */
    private static void runConsumersWithKeysAndTimeout(Integer consumerCount, Integer timeOutMinutes) throws InterruptedException {

        ExecutorService executorService = Executors.newFixedThreadPool(consumerCount + 1);
        executorService.execute(IntegrationKafkaApplication::runProducerWithKeys);

        executorService.awaitTermination(timeOutMinutes, TimeUnit.MINUTES);

        for (int i = 0; i < consumerCount; i++) {
            String consumerId = Integer.toString(i + 1);
            executorService.execute(() -> startConsumer(consumerId, config.getGroupIdConfig()));
        }

        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.MINUTES);
    }
    /**
     * For 3 case
     */
    private static void runConsumersWithVersionsAndTimeout(Integer consumerCount, Integer timeOutMinutes) throws InterruptedException {

        ExecutorService executorService = Executors.newFixedThreadPool(consumerCount + 1);
        executorService.execute(IntegrationKafkaApplication::runProducerWithVersions);

        executorService.awaitTermination(timeOutMinutes, TimeUnit.MINUTES);

        for (int i = 0; i < consumerCount; i++) {
            String consumerId = Integer.toString(i + 1);
            executorService.execute(() -> startConsumer(consumerId, config.getGroupIdConfig()));
        }

        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.MINUTES);
    }

    private static void runProducer() {
        RoutingDocument document = loadDocument();
        Producer<String, RoutingDocument> producer = ProducerCreator.createProducer(config);
        for (int index = 0; index < config.getMessageCount(); index++) {
            document.setDocId(index);
            sendMessage(producer, new ProducerRecord<>(config.getTopicName(), document), index);
        }

    }

    private static void runProducerWithKeys() {
        RoutingDocument document = loadDocument();
        Producer<String, RoutingDocument> producer = ProducerCreator.createProducer(config);
        for (int index = 0; index < config.getMessageCount(); index++) {
            document.setDocId(index);
            sendMessage(producer, new ProducerRecord<>(config.getTopicName(), UUID.randomUUID().toString(), document), index);
        }
        producer.close();
    }

    /**
     * Генерация сообщений с тремя версиями на один ключ
     */
    private static void runProducerWithVersions() {
        RoutingDocument document = loadDocument();
        Producer<String, RoutingDocument> producer = ProducerCreator.createProducer(config);

        for (int index = 0; index < config.getMessageCount(); index++) {
            document.setDocId(index);
            document.setDocType(DocumentTypeEnum.unknown);
            sendMessage(producer, new ProducerRecord<>(config.getTopicName(), String.format("key_%s", index), document), index);
        }

        for (int index = 0; index < config.getMessageCount(); index++) {
            document.setDocId(index);
            document.setDocType(DocumentTypeEnum.order);
            sendMessage(producer, new ProducerRecord<>(config.getTopicName(), String.format("key_%s", index), document), index);
        }

        for (int index = 0; index < config.getMessageCount(); index++) {
            document.setDocId(index);
            document.setDocType(DocumentTypeEnum.invoice);
            sendMessage(producer, new ProducerRecord<>(config.getTopicName(), String.format("key_%s", index), document), index);
        }

    }

    private static RoutingDocument loadDocument() {
        String messageXml = "<?xml version=\"1.0\" encoding=\"windows-1251\"?><routingDocument><docId>1</docId><docType>order</docType></routingDocument>";
        RoutingDocument document;
        try {
            document = xmlMapper().readValue(messageXml, RoutingDocument.class);
        } catch (IOException e) {
            LOG.log(Level.WARNING, "Error readValue from resource", e);
            throw new TestMessageException(e);
        }
        return document;
    }

    private static void sendMessage(
            Producer<String, RoutingDocument> producer,
            ProducerRecord<String, RoutingDocument> record,
            Integer index
    ) {
        try {
            RecordMetadata metadata = producer.send(record).get();
            LOG.info(String.format("Record %s sent to partition=%s with key=%s, offset=%s", index, metadata.partition(), record.key(), metadata.offset()));
            msg_sent_counter.incrementAndGet();
        } catch (ExecutionException | InterruptedException e) {
            LOG.log(Level.WARNING, "Error in sending record", e);
        }
    }
}
