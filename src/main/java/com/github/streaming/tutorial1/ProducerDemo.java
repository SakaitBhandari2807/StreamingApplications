package com.github.streaming.tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {
    public static void main(String[] args) {
        System.out.println("Hello World!");
        String bootStrapServers ="localhost:9092";

        //Create producers properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootStrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //create the producers
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);

        //create a producer records

        ProducerRecord<String,String>record =
                new ProducerRecord<String, String>("first_topic","Hello World!");

        //send data
        producer.send(record);

        producer.flush();
        producer.close();

    }
}
