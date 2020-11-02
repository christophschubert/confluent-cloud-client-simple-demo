package net.christoph.schubert.confluent.cloud;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class SimpleProducer {
    public static void main(String[] args) throws ExecutionException, InterruptedException {

        final String bootstrapServer = "SASL_SSL://pkc-lq8gm.westeurope.azure.confluent.cloud:9092";

        final String apiKey = "";
        final String apiSecret = "";

        final String jaasConfig = String.format("org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";",
                apiKey, apiSecret);

        System.out.println(jaasConfig);

        final Properties settings = new Properties();
        settings.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        settings.setProperty("ssl.endpoint.identification.algorithm", "https");
        settings.setProperty("security.protocol", "SASL_SSL");
        settings.setProperty("sasl.mechanism", "PLAIN");
        settings.setProperty("sasl.jaas.config", jaasConfig);


        settings.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        settings.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        System.out.println("created producer");
        final KafkaProducer<String, String> producer = new KafkaProducer<>(settings);

        for (int i = 0; i < 30; ++i){
            final ProducerRecord<String, String> producerRecord = new ProducerRecord<>("test-topic", "hello" + i, "from Java producer" +i);
            final RecordMetadata recordMetadata = producer.send(producerRecord).get();
            final long offset = recordMetadata.offset();
            final int partition = recordMetadata.partition();
            System.out.println("offset " + offset + "/" + partition);
        }
        producer.close();
    }
}
