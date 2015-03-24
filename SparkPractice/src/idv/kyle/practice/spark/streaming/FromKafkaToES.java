package idv.kyle.practice.spark.streaming;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import kafka.serializer.StringDecoder;

import org.apache.commons.httpclient.DefaultHttpMethodRetryHandler;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
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
  static String esIndex = null;

  public static void main(String[] args) {
    if (args.length != 7) {
      System.err
          .println("Usage: JavaKafkaWordCount <zkQuorum> <group> <topics> <numThreads> <esNodes> <esIndex> <WALenabled>");
      System.exit(1);
    }

    SparkConf sparkConf = new SparkConf().setAppName("FromKafkaToES");
    if ("true".equals(args[6])) {
      sparkConf.set("spark.streaming.receiver.writeAheadLog.enable", "true");
    }
    sparkConf.set("es.index.auto.create", "true");
    sparkConf.set("es.nodes", args[4]);
    esIndex = args[5];
    // Create the context with a 1 second batch size
    JavaStreamingContext jssc =
        new JavaStreamingContext(sparkConf, new Duration(2000));
    if ("true".equals(args[6])) {
      jssc.checkpoint("/tmp/sparkcheckpoint");
    }

    int numThreads = Integer.parseInt(args[3]);
    Map<String, Integer> topicMap = new HashMap<String, Integer>();
    String[] topics = args[2].split(",");
    for (String topic : topics) {
      topicMap.put(topic, numThreads);
    }

    Map<String, String> kafkaParams = new HashMap<String, String>();
    kafkaParams.put("zookeeper.connect", args[0]);
    kafkaParams.put("group.id", args[1]);
    kafkaParams.put("serializer.class", "kafka.serializer.StringEncoder");
    kafkaParams.put("request.required.acks", "1");

    // kafkaParams.put("metadata.broker.list", "sparkvm.localhost:6667");
    // kafkaParams.put("auto.commit.interval.ms", "10000");
    // kafkaParams.put("consumer.timeout.ms", "15000");

    JavaPairReceiverInputDStream<String, String> messages =
        KafkaUtils.createStream(jssc, String.class, String.class,
            StringDecoder.class, StringDecoder.class, kafkaParams, topicMap,
            StorageLevel.MEMORY_AND_DISK_SER_2());

    JavaDStream<Map<String, String>> queryResult =
        messages
            .map(new Function<Tuple2<String, String>, Map<String, String>>() {
              @Override
              public Map<String, String> call(Tuple2<String, String> tuple2) {
                String item1 = tuple2._1();
                String url = tuple2._2();
                LOG.info("query proxy url : " + url + ", item1: " + item1);
                HttpClient httpclient = new HttpClient();
                GetMethod method = new GetMethod(url);
                method.getParams().setParameter(HttpMethodParams.RETRY_HANDLER,
                    new DefaultHttpMethodRetryHandler(3, false));
                Map<String, String> resultMap = new HashMap<String, String>();
                try {
                  int retry = 0;
                  while (retry <= 3) {
                    int statusCode = httpclient.executeMethod(method);
                    if (statusCode != HttpStatus.SC_OK) {
                      LOG.warn("Method failed: " + method.getStatusLine());
                    }
                    InputStream resStream = method.getResponseBodyAsStream();
                    BufferedReader br =
                        new BufferedReader(new InputStreamReader(resStream));
                    StringBuffer resBuffer = new StringBuffer();
                    try {
                      String resTemp = "";
                      while ((resTemp = br.readLine()) != null) {
                        resBuffer.append(resTemp);
                      }
                    } catch (Exception e) {
                      e.printStackTrace();
                    } finally {
                      br.close();
                    }
                    String queryProxyResult = resBuffer.toString();
                    // LOG.info("query proxy result: " + queryProxyResult);
                    JSONObject jsonObj = new JSONObject(queryProxyResult);
                    if ("200".equals(jsonObj.get("status").toString())) {
                      Iterator<String> keys = jsonObj.keys();
                      while (keys.hasNext()) {
                        String keyValue = (String) keys.next();
                        String valueString = jsonObj.getString(keyValue);
                        resultMap.put(keyValue, valueString);
                      }
                      return resultMap;
                    } else {
                      retry++;
                      LOG.warn("result status is not ok. status : "
                          + jsonObj.get("status").toString() + ", retry: "
                          + retry);
                    }
                  }
                } catch (Exception e) {
                  e.printStackTrace();
                } finally {
                  method.releaseConnection();
                }
                return resultMap;
              }
            });

    queryResult.foreach(new Function<JavaRDD<Map<String, String>>, Void>() {
      private static final long serialVersionUID = 6272424972267329328L;

      @Override
      public Void call(JavaRDD<Map<String, String>> rdd) throws Exception {
        LOG.info("ES index: " + esIndex);
        JavaEsSpark.saveToEs(rdd, esIndex);
        return (Void) null;
      }
    });

    jssc.start();
    jssc.awaitTermination();
  }
}
