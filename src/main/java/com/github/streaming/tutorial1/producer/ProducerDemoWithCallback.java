package com.github.streaming.tutorial1.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {
    public static void main(String[] args) {

        //TODO:create a logger for this class
        Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getName());


        String bootStrapServers ="localhost:9092";

        //TODO:Create producers properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootStrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //TODO:create the producers
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);

        //TODO:create a producer records

        ProducerRecord<String,String> record =
                new ProducerRecord<String, String>("first_topic","Hello World!");

        // TODO: send data and provide callback
        producer.send(record,(recordMetadata,e)->{
            if(e==null){
                logger.info("Received new metadata \n"+
                        "Topic: "+recordMetadata.topic()+"\n"+
                        "Partition: "+recordMetadata.partition()+"\n"+
                        "Offset: "+recordMetadata.offset()+"\n"+
                        "Timestamp: "+recordMetadata.timestamp());
            }
            else{
                logger.error("Error occurred while producing"+e);
            }
        });

        producer.flush();
        producer.close();

    }
}
