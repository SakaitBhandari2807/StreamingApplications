package streaming.tutorial1.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoAssignSeek {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemo.class.getName());
        String bootStrapServers ="localhost:9092";

        //TODO:Create consumer properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootStrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        //properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"group-2");


        //TODO:create the consumers
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);

        String topic_name = "first_topic";

        //TODO:assign
        TopicPartition topicPartition = new TopicPartition(topic_name,1);

        consumer.assign(Arrays.asList(topicPartition));

        //TODO:seek
        consumer.seek(topicPartition,15L);
        int numberOfMessagesToRead = 5 ;
        boolean keepOnReading = true;
        int numberOfMessageReadSoFar = 0;

        //TODO:get data
        while(keepOnReading){
            ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(100));
            for(ConsumerRecord record :records){
                numberOfMessageReadSoFar+=1;
                logger.info("Key: "+record.key()+" value: "+record.value());
                logger.info("Partition: "+record.partition()+" offset: "+record.offset());
                if(numberOfMessageReadSoFar>numberOfMessagesToRead){
                    keepOnReading = false;
                    break;
                }
            }
        }

    }
}
