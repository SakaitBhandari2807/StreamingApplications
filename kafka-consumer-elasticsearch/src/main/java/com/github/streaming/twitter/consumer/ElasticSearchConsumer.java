package com.github.streaming.twitter.consumer;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ElasticSearchConsumer {


   static Properties properties = new Properties();
   static String qualifiedFileName="./kafka-consumer-elasticsearch/src/main/resources/elasticsearch.properties";

  public static void loadCredentials(){
      InputStream is=null;
      try {
          is = new FileInputStream(qualifiedFileName);
          properties.load(is);
      } catch (IOException e) {
          e.printStackTrace();
      }
  }


  public static RestHighLevelClient createClient(){
      //String hostname="";
      CredentialsProvider credentialsProvider =  new BasicCredentialsProvider();
      credentialsProvider.setCredentials(AuthScope.ANY,
              new UsernamePasswordCredentials(properties.getProperty("username"),properties.getProperty("password"))
      );
      RestHighLevelClient client = new RestHighLevelClient(
              RestClient.builder(new HttpHost(properties.getProperty("hostname"),443,"https"))
                      .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                          @Override
                          public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                              return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                          }
                      }));
      return client;
  }

  public static KafkaConsumer<String,String> createConsumer(String topic){
      String bootstrapServers="localhost:9092";
      String groupId="twitter-consumer-elasticgroup";
      Properties properties = new Properties();
      properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
      properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
      properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
      properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
      properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
      properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");
      properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,"10");

      KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);
      consumer.subscribe(Arrays.asList(topic));
      return consumer;
  }

  public static void main(String args[]) throws IOException {
      loadCredentials();
      Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());
      RestHighLevelClient client = createClient();

      KafkaConsumer<String,String>consumer = createConsumer("twitter_tweets");
      while(true){
          ConsumerRecords<String,String>records = consumer.poll(Duration.ofMillis(100));


          for(ConsumerRecord<String,String>record :records){
              //TODO:Create a strategy to create an idempotent consumer
              String id = extractIdFromTweet(record.value());
              IndexRequest request = new IndexRequest("twitter");
              request.id(id);
              request.source(record.value(),XContentType.JSON);
              IndexResponse response = client.index(request,RequestOptions.DEFAULT);
              logger.info(response.getId());
              try {
                  Thread.sleep(1000);
              } catch (InterruptedException e) {
                  e.printStackTrace();
              }
          }
      }
      //client.close();
  }
   private static JsonParser jsonParser = new JsonParser();
    private static String extractIdFromTweet(String tweetJson) {
        //gson library
        return jsonParser.parse(tweetJson).getAsJsonObject().get("id_str").getAsString();
    }
}
