package com.github.streaming.twitter.consumer;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
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

      RestHighLevelClient client = new RestHighLevelClient(
              RestClient.builder(new HttpHost(properties.getProperty("hostname"),443,"https")));

      CredentialsProvider credentialsProvider =  new BasicCredentialsProvider();
      credentialsProvider.setCredentials(AuthScope.ANY,
              new UsernamePasswordCredentials(properties.getProperty("username"),properties.getProperty("password"))
      );
      return client;
  }


  public static void main(String args[]) throws IOException {
      loadCredentials();
      Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());
      RestHighLevelClient client = createClient();
      String jsonString="{ \"name\" : \"Sakait Bhandari\" }";
      IndexRequest indexRequest = new IndexRequest("twitter").source(jsonString, XContentType.JSON);

      IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
      String id = indexResponse.getId();
      logger.info("id:",id);

      client.close();


  }
}
