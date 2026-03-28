package com.linuxacademy.ccdak.kafkaJavaConnect;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.Future;

public class Main {

    public static void main(String[] args) {
        Properties props =  new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 5000);
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        try(Producer<String, String> producer = new KafkaProducer<>(props)){
            for(int i = 0; i < 1000; i+=5){
                String randomString = RandomStringUtils.randomAlphabetic(1);
                System.out.println(i + "->" + randomString);
                Future<RecordMetadata> future = producer.send(new ProducerRecord<>("streams-input-topic", Integer.toString(i), randomString));
                RecordMetadata metadata = future.get();
                System.out.println(metadata);
                Thread.sleep(1000);
            }
        }catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
}
