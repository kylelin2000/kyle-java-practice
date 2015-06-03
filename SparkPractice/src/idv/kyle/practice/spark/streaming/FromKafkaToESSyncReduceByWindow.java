package idv.kyle.practice.spark.streaming;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.serializer.StringDecoder;

import org.apache.commons.httpclient.DefaultHttpMethodRetryHandler;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.codehaus.jettison.json.JSONException;
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

public class FromKafkaToESSyncReduceByWindow {
  private static final Logger LOG = LoggerFactory
      .getLogger(FromKafkaToESSyncReduceByWindow.class);
  static String esIndex = null;

  public static void main(String[] args) throws Exception {
    String esNodes = "";
    String threadNumber = "";
    String kafkaTopics = "";
    String zkHosts = "";
    String kafkaGroup = "";
    String reqAcks = "0";
    int batchDuration = 2000;
    int windowDuration = 2000;
    int slideDuration = 2000;

    Properties prop = new Properties();
    Path pt = new Path(ConstantUtil.propertiesFileName);
    FileSystem fs = FileSystem.get(new Configuration());
    prop.load(new InputStreamReader(fs.open(pt)));
    zkHosts = prop.getProperty("zookeeper.host");
    kafkaGroup = prop.getProperty("kafka.group");
    kafkaTopics = prop.getProperty("kafka.topics");
    threadNumber = prop.getProperty("spark.kafka.thread.num");
    esNodes = prop.getProperty("es.nodes");
    esIndex = prop.getProperty("es.index");
    reqAcks = prop.getProperty("kafka.request.required.acks");
    batchDuration = Integer.parseInt(prop.getProperty("spark.stream.batch.duration.ms"));
    windowDuration =
        Integer.parseInt(prop.getProperty("spark.stream.window.duration.ms"));
    slideDuration =
        Integer.parseInt(prop.getProperty("spark.stream.slide.duration.ms"));
    
    LOG.info("read properties: zkHosts=" + zkHosts + ", kafkaTopics=" + kafkaTopics + ", esIndex=" + esIndex);

    SparkConf sparkConf = new SparkConf().setAppName("FromKafkaToES-Sync");
    sparkConf.set("es.index.auto.create", "true");
    sparkConf.set("es.nodes", esNodes);
    sparkConf.set("spark.locality.wait", "500");
    sparkConf.set("spark.streaming.blockInterval", "1000");
    JavaStreamingContext jssc =
        new JavaStreamingContext(sparkConf, new Duration(batchDuration));

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
    kafkaParams.put("request.required.acks", reqAcks);

    int numStreams = 3;
    List<JavaPairDStream<String, String>> kafkaStreams =
        new ArrayList<JavaPairDStream<String, String>>(numStreams);
    for (int i = 0; i < numStreams; i++) {
      kafkaStreams.add(KafkaUtils.createStream(jssc, String.class,
          String.class, StringDecoder.class, StringDecoder.class, kafkaParams,
          topicMap, StorageLevel.MEMORY_AND_DISK_SER_2()));
    }
    JavaPairDStream<String, String> unifiedStream =
        jssc.union(kafkaStreams.get(0),
            kafkaStreams.subList(1, kafkaStreams.size()));

    /*
    JavaPairReceiverInputDStream<String, String> messages =
        KafkaUtils.createStream(jssc, String.class, String.class,
            StringDecoder.class, StringDecoder.class, kafkaParams, topicMap,
            StorageLevel.MEMORY_AND_DISK_SER_2());
    */

    JavaDStream<String> lines =
        unifiedStream.map(new Function<Tuple2<String, String>, String>() {
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
        }, new Duration(windowDuration), new Duration(slideDuration));

    JavaDStream<String> queryResults =
        streams.flatMap(new FlatMapFunction<String, String>() {
          private static final long serialVersionUID = 6272424972267329328L;

          @Override
          public Iterable<String> call(String inputMessage) {
            Properties prop = new Properties();
            Path pt = new Path(ConstantUtil.propertiesFileName);
            try {
              FileSystem fs = FileSystem.get(new Configuration());
              prop.load(new InputStreamReader(fs.open(pt)));
            } catch (IOException e1) {
              e1.printStackTrace();
            }
            String queryProxyUrl = prop.getProperty("queryproxy.url.batch");

            HttpClient httpclient = new HttpClient();
            PostMethod method = new PostMethod(queryProxyUrl);

            try {
              String[] lines = inputMessage.trim().split("\n");
              StringBuffer requestContent = new StringBuffer();
              LOG.info("use POST to send " + lines.length + " query request to query proxy");
              for (String line : lines) {
                try {
                  JSONObject jsonObj = new JSONObject(line);
                  SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                  String context =
                      jsonObj.get("context") + ", spark read from kafka at "
                          + sdf.format(new Date());
                  jsonObj.put("context", context);
                  requestContent.append("\n" + jsonObj.toString());
                } catch (JSONException jsonex) {
                  LOG.warn("Not send request to query proxy. Not a valid json string: " + line);
                }
              }
              String postBody = requestContent.toString().trim();
              if (!"".equals(postBody) || !postBody.isEmpty()) {
                StringRequestEntity requestEntity =
                    new StringRequestEntity(postBody, "application/json",
                        "UTF-8");
                method.setRequestEntity(requestEntity);
                method.getParams().setParameter(HttpMethodParams.RETRY_HANDLER,
                    new DefaultHttpMethodRetryHandler(3, false));

                int statusCode = httpclient.executeMethod(method);
                LOG.info("query proxy response code: " + statusCode);
                if (statusCode != HttpStatus.SC_OK) {
                  LOG.warn("Method failed: " + method.getStatusLine());
                }
                InputStream resStream = method.getResponseBodyAsStream();
                BufferedReader br =
                    new BufferedReader(new InputStreamReader(resStream));
                StringBuffer resBuffer = new StringBuffer();
                List<String> results = new ArrayList<String>();
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
            } catch (Exception e) {
              e.printStackTrace();
            } finally {
              method.releaseConnection();
            }
            LOG.info("should not run here");
            return new ArrayList<String>();
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
                  SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                  jsonObj.append("returnTimeFromQueryProxy", sdf.format(new Date()));
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
