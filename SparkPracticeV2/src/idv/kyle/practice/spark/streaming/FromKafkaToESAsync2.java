package idv.kyle.practice.spark.streaming;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;

import kafka.serializer.StringDecoder;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.codehaus.jettison.json.JSONObject;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

/**
 * Usage: ./bin/spark-submit --class
 * idv.kyle.practice.spark.streaming.JavaKafkaWordCount
 * /root/StormPractice-0.0.1-jar-with-dependencies.jar config.properties
 * 
 */

public class FromKafkaToESAsync2 {
  private static final Logger LOG = LoggerFactory
      .getLogger(FromKafkaToESAsync2.class);
  static String esIndex = null;
  static String queryProxyUrl = "";

  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      System.err.println("Usage: JavaKafkaWordCount <config_file_path>");
      System.exit(1);
    }

    String esNodes = "";
    String threadNumber = "";
    String kafkaTopics = "";
    String zkHosts = "";
    String kafkaGroup = "";
    String walEnabled = "";

    Properties prop = new Properties();
    InputStream input = null;
    try {
      File file = new File(args[0]);
      LOG.info("read property file: " + file.getPath());
      if (!file.exists()) {
        LOG.error("file not found: " + file.getPath());
      }
      input = new FileInputStream(file);
      prop.load(input);
      zkHosts = prop.getProperty("zookeeper.host");
      kafkaGroup = prop.getProperty("kafka.group");
      kafkaTopics = prop.getProperty("kafka.topics");
      threadNumber = prop.getProperty("spark.kafka.thread.num");
      esNodes = prop.getProperty("es.nodes");
      esIndex = prop.getProperty("es.index");
      walEnabled = prop.getProperty("spark.WAL.enabled");
      queryProxyUrl = prop.getProperty("queryproxy.url.batch");
    } finally {
      if (input != null) {
        input.close();
      }
    }

    LOG.info("query proxy url: " + queryProxyUrl);

    SparkConf sparkConf = new SparkConf().setAppName("FromKafkaToES-Async");
    if ("true".equals(walEnabled)) {
      sparkConf.set("spark.streaming.receiver.writeAheadLog.enable", "true");
    }
    sparkConf.set("es.index.auto.create", "true");
    // sparkConf.get("spark.yarn.user.classpath.first", "true");
    sparkConf.set("es.nodes", esNodes);
    JavaStreamingContext jssc =
        new JavaStreamingContext(sparkConf, new Duration(2000));
    if ("true".equals(walEnabled)) {
      jssc.checkpoint("/tmp/sparkcheckpoint");
    }

    int numThreads = Integer.parseInt(threadNumber);
    Map<String, Integer> topicMap = new HashMap<String, Integer>();
    String[] topics = kafkaTopics.split(",");
    for (String topic : topics) {
      topicMap.put(topic, numThreads);
    }

    Map<String, String> kafkaParams = new HashMap<String, String>();
    kafkaParams.put("zookeeper.connect", zkHosts);
    kafkaParams.put("group.id", kafkaGroup);
    kafkaParams.put("serializer.class", "kafka.serializer.StringEncoder");

    JavaPairReceiverInputDStream<String, String> messages =
        KafkaUtils.createStream(jssc, String.class, String.class,
            StringDecoder.class, StringDecoder.class, kafkaParams, topicMap,
            StorageLevel.MEMORY_AND_DISK_SER_2());

    JavaDStream<String> lines =
        messages.map(new Function<Tuple2<String, String>, String>() {
          private static final long serialVersionUID = 1L;

          @Override
          public String call(Tuple2<String, String> tuple2) {
            String item2 = tuple2._2();
            return item2;
          }
        });

    JavaDStream<String> streams =
        lines.reduceByWindow(new Function2<String, String, String>() {
          private static final long serialVersionUID = -6486260869365466022L;

          @Override
          public String call(String str1, String str2) {
            return str1 + "\n" + str2;
          }
        }, new Function2<String, String, String>() {
          private static final long serialVersionUID = -6486260869365466022L;

          @Override
          public String call(String str1, String str2) {
            return str1.substring(0, str1.lastIndexOf(str2));
          }
        }, new Duration(2000), new Duration(2000));

    JavaDStream<String> queryResults =
        streams.flatMap(new FlatMapFunction<String, String>() {
          private static final long serialVersionUID = 6272424972267329328L;

          @Override
          public Iterable<String> call(String postBody) {
            LOG.info("query proxy url: " + queryProxyUrl + ", postBody: "
                + postBody.trim());

            List<String> results = new ArrayList<String>();

            ClassLoader classLoader = this.getClass().getClassLoader();
            URL resource =
                classLoader
                    .getResource("org/apache/http/impl/nio/codecs/DefaultHttpRequestWriterFactory.class");
            System.out.println("resource ======> " + resource);
            CloseableHttpAsyncClient httpclient =
                HttpAsyncClients.createDefault();
            try {
              httpclient.start();
              HttpPost request =
                  new HttpPost("http://10.1.193.226:9090/v1/_bulk_tag");
              StringEntity params = new StringEntity(postBody.trim());
              request.setEntity(params);
              Future<HttpResponse> future = httpclient.execute(request, null);
              HttpResponse response = future.get();
              StatusLine status = response.getStatusLine();
              LOG.info("query proxy response status : " + status);
              if (status.getStatusCode() == HttpStatus.SC_OK) {
                HttpEntity responseEntity = response.getEntity();
                if (responseEntity != null) {
                  BufferedReader br =
                      new BufferedReader(new InputStreamReader(responseEntity
                          .getContent()));
                  StringBuffer resBuffer = new StringBuffer();
                  try {
                    String resTemp = "";
                    while ((resTemp = br.readLine()) != null) {
                      LOG.info("query proxy result line: " + resTemp);
                      results.add(resTemp);
                      resBuffer.append(resTemp);
                    }
                  } catch (Exception e) {
                    e.printStackTrace();
                  } finally {
                    br.close();
                  }
                  String queryProxyResult = resBuffer.toString();
                  LOG.info("query proxy result: " + queryProxyResult);
                  return results;
                }
              } else {
                LOG.warn("query fail. status : " + status);
              }
            } catch (Exception e) {
              e.printStackTrace();
            } finally {
              try {
                httpclient.close();
              } catch (IOException e) {
                e.printStackTrace();
              }
            }

            return results;
          }
        });

    JavaDStream<Map<String, String>> queryResult =
        queryResults.map(new Function<String, Map<String, String>>() {
          @Override
          public Map<String, String> call(String line) {
            Map<String, String> resultMap = new HashMap<String, String>();
            String result = line.toString();
            LOG.info("query proxy return single result: " + result);
            if (!result.isEmpty()) {
              try {
                JSONObject jsonObj = new JSONObject(result);
                if ("200".equals(jsonObj.get("status").toString())) {
                  Iterator<String> keys = jsonObj.keys();
                  while (keys.hasNext()) {
                    String keyValue = (String) keys.next();
                    String valueString = jsonObj.getString(keyValue);
                    resultMap.put(keyValue, valueString);
                  }
                  return resultMap;
                } else {
                  LOG.error("query proxy result status is not 200");
                }
              } catch (Exception e) {
                e.printStackTrace();
              }
            }
            return resultMap;
          }
        });

    queryResult.foreach(new Function<JavaRDD<Map<String, String>>, Void>() {
      private static final long serialVersionUID = 6272424972267329328L;

      @Override
      public Void call(JavaRDD<Map<String, String>> rdd) throws Exception {
        List<Map<String, String>> collect = rdd.collect();
        LOG.info("collect size: " + collect.size());
        if (collect.size() > 1) {
          for (Map<String, String> map : collect) {
            if (map.size() > 0) {
              LOG.info("ES index: " + esIndex);
              JavaEsSpark.saveToEs(rdd, esIndex);
              break;
            }
          }
        }

        return (Void) null;
      }
    });

    jssc.start();
    jssc.awaitTermination();
  }
}
