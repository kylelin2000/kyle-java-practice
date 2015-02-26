package idv.kyle.practice.spark.streaming;

import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

/**
 * Usage: ./bin/spark-submit --class
 * idv.kyle.practice.spark.streaming.JavaKafkaWordCount
 * /root/StormPractice-0.0.1-jar-with-dependencies.jar localhost:2181
 * kafka-spark-1 tp001 1
 * 
 */

public class FromKafkaToES {
  private static final Logger LOG = LoggerFactory
      .getLogger(FromKafkaToES.class);

  public static void main(String[] args) {
    if (args.length < 4) {
      System.err
          .println("Usage: JavaKafkaWordCount <zkQuorum> <group> <topics> <numThreads>");
      System.exit(1);
    }

    SparkConf sparkConf = new SparkConf().setAppName("FromKafkaToES");
    sparkConf.set("es.index.auto.create", "true");
    // Create the context with a 1 second batch size
    JavaStreamingContext jssc =
        new JavaStreamingContext(sparkConf, new Duration(2000));

    int numThreads = Integer.parseInt(args[3]);
    Map<String, Integer> topicMap = new HashMap<String, Integer>();
    String[] topics = args[2].split(",");
    for (String topic : topics) {
      topicMap.put(topic, numThreads);
    }

    JavaPairReceiverInputDStream<String, String> messages =
        KafkaUtils.createStream(jssc, args[0], args[1], topicMap);

    JavaDStream<String> queryResult =
        messages.map(new Function<Tuple2<String, String>, String>() {
          @Override
          public String call(Tuple2<String, String> tuple2) {
            String url = tuple2._2();
            ClassLoader classLoader = FromKafkaToES.class.getClassLoader();
            URL resource = classLoader.getResource("org/apache/http/impl/client/HttpClientBuilder.class");
            LOG.info(resource.toString());
            LOG.info("query proxy url : " + url);
            CloseableHttpClient httpclient = HttpClientBuilder.create().build();
            try {
              int retry = 0;
              while (retry <= 3) {
                HttpGet httpget = new HttpGet(url);
                HttpResponse response = httpclient.execute(httpget);
                int status = response.getStatusLine().getStatusCode();
                LOG.info("Response status : " + status);
                if (status == HttpStatus.SC_OK) {
                  HttpEntity entity = response.getEntity();
                  String queryProxyResult = EntityUtils.toString(entity);
                  LOG.info("result from query proxy : " + queryProxyResult);
                  JSONObject jsonObj = new JSONObject(queryProxyResult);
                  if ("200".equals(jsonObj.get("status").toString())) {
                    return jsonObj.toString();
                  } else {
                    retry++;
                    LOG.warn("result status is not ok. status : "
                        + jsonObj.get("status").toString() + ", retry: "
                        + retry);
                  }
                } else {
                  LOG.warn("query fail. status : " + status);
                }
              }
            } catch (Exception e) {
              e.printStackTrace();
            } finally {
              try {
                httpclient.close();
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            }
            return url;
          }
        });

    queryResult.print();
    jssc.start();
    jssc.awaitTermination();
  }
}
