package idv.kyle.practice.spark.streaming;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import kafka.serializer.StringDecoder;

import org.apache.commons.httpclient.DefaultHttpMethodRetryHandler;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
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

public class FromKafkaToES3 {
  private static final Logger LOG = LoggerFactory
      .getLogger(FromKafkaToES3.class);
  static String esIndex = null;

  public static void main(String[] args) {
    if (args.length != 7) {
      System.err
          .println("Usage: JavaKafkaWordCount <zkQuorum> <group> <topics> <numThreads> <esNodes> <esIndex> <WALenabled>");
      System.exit(1);
    }

    SparkConf sparkConf = new SparkConf().setAppName("FromKafkaToES2");
    if ("true".equals(args[6])) {
      sparkConf.set("spark.streaming.receiver.writeAheadLog.enable", "true");
    }
    sparkConf.set("es.index.auto.create", "true");
    sparkConf.set("es.nodes", args[4]);
    esIndex = args[5];
    // Create the context with a 1 second batch size
    JavaStreamingContext jssc =
        new JavaStreamingContext(sparkConf, new Duration(1000));
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

    JavaPairReceiverInputDStream<String, String> messages =
        KafkaUtils.createStream(jssc, String.class, String.class,
            StringDecoder.class, StringDecoder.class, kafkaParams, topicMap,
            StorageLevel.MEMORY_AND_DISK_SER_2());

    JavaPairDStream<String, String> streams =
        messages.window(new Duration(20000));

    JavaDStream<List<String>> queryResult =
        streams.map(new Function<Tuple2<String, String>, List<String>>() {
          @Override
          public List<String> call(Tuple2<String, String> tuple2) {
            String item1 = tuple2._1();
            String postBody = tuple2._2();
            LOG.info("query proxy postBody : " + postBody + ", item1: " + item1);

            Map<String, String> resultMap = new HashMap<String, String>();
            HttpClient httpclient = new HttpClient();
            PostMethod method =
                new PostMethod("http://10.1.192.49:9090/v1/_bulk_tag");

            try {
              StringRequestEntity requestEntity =
                  new StringRequestEntity(postBody, "application/json", "UTF-8");
              method.setRequestEntity(requestEntity);
              method.getParams().setParameter(HttpMethodParams.RETRY_HANDLER,
                  new DefaultHttpMethodRetryHandler(3, false));

              int statusCode = httpclient.executeMethod(method);
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
                  LOG.info("result line: " + resTemp);
                  results.add(resTemp);
                  resBuffer.append(resTemp);
                }
              } catch (Exception e) {
                e.printStackTrace();
              } finally {
                br.close();
              }
              String queryProxyResult = resBuffer.toString();
              LOG.info("queryProxyResult: " + queryProxyResult);
              return Arrays.asList(queryProxyResult.split("\n"));
            } catch (Exception e) {
              e.printStackTrace();
            } finally {
              method.releaseConnection();
            }
            return new ArrayList<String>();
          }
        });

    queryResult.print();

//    resultItem.foreach(new Function<JavaRDD<String>, Void>() {
//      private static final long serialVersionUID = 6272424972267329328L;
//
//      @Override
//      public Void call(JavaRDD<String> rdd) throws Exception {
//        LOG.info("ES index: " + esIndex);
//        List<String> list = rdd.collect();
//        if (!list.isEmpty()) {
//          for (String item : list) {
//            LOG.info("query result item: " + item);
//          }
//        }
//        return (Void) null;
//      }
//    });

    jssc.start();
    jssc.awaitTermination();
  }
}
