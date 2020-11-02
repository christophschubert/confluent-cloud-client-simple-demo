package net.christoph.schubert.confluent.cloud;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class SimpleConsumer {
    public static void main(String[] args) {

        final String bootstrapServer = "SASL_SSL://pkc-lq8gm.westeurope.azure.confluent.cloud:9092";

        final String apiKey = "";
        final String apiSecret = "";
        final String jaasConfig = String.format("org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";",
                apiKey, apiSecret);
        

        final Properties settings = new Properties();
        settings.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        settings.setProperty("ssl.endpoint.identification.algorithm", "https");
        settings.setProperty( "security.protocol", "SASL_SSL");
        settings.setProperty(SaslConfigs.SASL_MECHANISM, "PLAIN");
        settings.setProperty(SaslConfigs.SASL_JAAS_CONFIG, jaasConfig);
        settings.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-3");
        settings.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        settings.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        settings.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        settings.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(settings);
        consumer.subscribe(Collections.singleton("from-cli-default"));

        while(true) {
            final ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record.partition() + " " + record.offset() + " - " + record.key() + " " + record.value());
            }
        }
    }
}
