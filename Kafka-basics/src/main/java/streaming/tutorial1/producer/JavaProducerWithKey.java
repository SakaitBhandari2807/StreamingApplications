package streaming.tutorial1.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class JavaProducerWithKey {
    public static void main(String[] args) {

        //TODO:create a logger instance for this class
        Logger logger = LoggerFactory.getLogger(JavaProducerWithKey.class.getName());

        //TODO:create properties for kafka producer
        String bootStrapServers ="localhost:9092";

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootStrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //TODO:create a producer

        KafkaProducer<String,String>producer = new KafkaProducer<String, String>(properties);
        for(int i=0;i<10;i++) {
            //TODO:send message
            String key = "id_"+Integer.toString(i);
            String value = "value_"+Integer.toString(i);
            ProducerRecord<String, String> record = new ProducerRecord<>("first_topic",key,value);
            producer.send(record,(recordMetadata, e) -> {
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

        }
        producer.close();
    }
}
