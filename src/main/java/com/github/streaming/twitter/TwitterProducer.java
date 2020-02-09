package com.github.streaming.twitter;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());
    String consumerKey="zxUqDonY95a0yPSrQKbqlNXqn";
    String consumerSecret="1f0m7iA5faoCRlqu8vegTr4gITxNWSHPA7o2zQzVlXgJX3vbSo";
    String token="1112730309907415042-zbhVDlcPwOblRF7f1mHwVScDH2YwbD";
    String secret="lWYSiALbyaIrFZPxRUhkGqFWKxKDZsAHaCSybYawi5Hq6";


    public TwitterProducer() {
    }

    public static void main(String[] args) {
        System.out.println("Twitter Application");
        new TwitterProducer().run();

    }
    public void run(){
        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);
        //TODO:Create a twitter client
        Client client = createTwitterClient(msgQueue);

        //TODO:establish connection
        client.connect();

        //TODO:create a kafka producer
        KafkaProducer<String,String> producer = createKafkaProducer();

        //TODO:Read twitter tweets
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
            if(msg!=null){
                logger.info(msg);
                producer.send(new ProducerRecord<String,String>("twitter_tweets", null,msg),(recordMetadata,e)->{
                        if(e!=null){
                            logger.error("Something bad happened",e);
                        }
                });
            }
        }
        logger.info("End of the application");
    }

    private KafkaProducer<String, String> createKafkaProducer() {
        String bootStrapServers ="localhost:9092";

        //Create producers properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootStrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //create the producers
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);
        return producer;
    }

    public Client createTwitterClient(BlockingQueue<String>msgQueue){



        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        // Optional: set up some followings and track terms

        List<String> terms = Lists.newArrayList("AAP");
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);


        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue))
                ;                         // optional: use this if you want to process client events

        Client hosebirdClient = builder.build();
        // Attempts to establish a connection.
        return hosebirdClient;
    }
}
