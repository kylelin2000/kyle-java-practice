package idv.kyle.practice.spark.streaming;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
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
import org.codehaus.jettison.json.JSONException;
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

public class FromKafkaToES2 {
  private static final Logger LOG = LoggerFactory
      .getLogger(FromKafkaToES2.class);
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

    int messageCount = 5;
    List<JSONObject> list = new ArrayList<JSONObject>();
    for (int i = 0; i < messageCount; i++) {
      LOG.info("read message from kafka. i: " + i);
      JavaPairReceiverInputDStream<String, String> message =
          KafkaUtils.createStream(jssc, String.class, String.class,
              StringDecoder.class, StringDecoder.class, kafkaParams, topicMap,
              StorageLevel.MEMORY_AND_DISK_SER_2());

      LOG.info("process message from Kafka");
      JavaDStream<JSONObject> jsonObj =
          message.map(new Function<Tuple2<String, String>, JSONObject>() {
            @Override
            public JSONObject call(Tuple2<String, String> tuple2) {
              String jsonStr = tuple2._2();
              LOG.info("query proxy jsonStr : " + jsonStr);
              try {
                return new JSONObject(jsonStr);
              } catch (JSONException e) {
                LOG.error(e.toString());
                return null;
              }
            }
          });

      jsonObj.foreach(new Function<JavaRDD<JSONObject>, Void>() {
        private static final long serialVersionUID = 6272424972267329328L;

        @Override
        public Void call(JavaRDD<JSONObject> rdd) throws Exception {
          List<JSONObject> jsonObjs = rdd.toArray();
          for (JSONObject jsonObj : jsonObjs) {
            LOG.info("jsonObj: " + jsonObj.toString());
          }
          return (Void) null;
        }
      });
    }

    jssc.start();
    jssc.awaitTermination();
  }
}
