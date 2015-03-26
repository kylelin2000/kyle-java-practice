package idv.kyle.practice.spark.streaming;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
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
import org.apache.spark.api.java.function.Function;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
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
    List<JavaPairDStream<String, String>> streamList =
        new ArrayList<JavaPairDStream<String, String>>();
    for (int i = 0; i < messageCount; i++) {
      LOG.info("read message from Kafka. i: " + i);
      streamList.add(KafkaUtils.createStream(jssc, String.class, String.class,
          StringDecoder.class, StringDecoder.class, kafkaParams, topicMap,
          StorageLevel.MEMORY_AND_DISK_SER_2()));
    }

    JavaPairDStream<String, String> stream =
        jssc.union(streamList.get(0), streamList.subList(1, streamList.size()));

    LOG.info("process message from Kafka");
    JavaDStream<String> jsonStr =
        stream.map(new Function<Tuple2<String, String>, String>() {
          private static final long serialVersionUID = -1952817916007854275L;

          @Override
          public String call(Tuple2<String, String> tuple2) {
            String jsonStr = tuple2._2();
            LOG.info("query proxy jsonStr : " + jsonStr);
            LOG.error("query proxy jsonStr : " + jsonStr);
            LOG.warn("query proxy jsonStr : " + jsonStr);
            LOG.info("query proxy jsonStr : " + jsonStr);
            LOG.error("query proxy jsonStr : " + jsonStr);
            LOG.warn("query proxy jsonStr : " + jsonStr);
            return jsonStr;
          }
        });

    jsonStr.foreach(new Function<JavaRDD<String>, Void>() {
      private static final long serialVersionUID = 6272424972267329328L;

      @Override
      public Void call(JavaRDD<String> rdd) throws Exception {
        List<String> list = rdd.collect();
        if (!list.isEmpty()) {
          LOG.info("ES index: " + esIndex);
          StringBuffer postBody = new StringBuffer();
          for (String item : list) {
            postBody.append(item + "\n");
          }
          HttpClient httpclient = new HttpClient();
          PostMethod method =
              new PostMethod("http://10.1.192.49:9090/v1/_bulk_tag");
          StringRequestEntity requestEntity =
              new StringRequestEntity(postBody.toString(), "application/json",
                  "UTF-8");
          method.setRequestEntity(requestEntity);
          method.getParams().setParameter(HttpMethodParams.RETRY_HANDLER,
              new DefaultHttpMethodRetryHandler(3, false));

          Client client =
              new TransportClient()
                  .addTransportAddress(new InetSocketTransportAddress(
                      "sparkvm.localdomain", 9300));
          try {
            String[] idx = esIndex.split("/");
            BulkRequestBuilder bulkRequest = client.prepareBulk();
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
                LOG.info("result line: " + resTemp);
                bulkRequest.add(client.prepareIndex(idx[0], idx[1], "1")
                    .setSource(resTemp));
                resBuffer.append(resTemp);
              }
            } catch (Exception e) {
              e.printStackTrace();
            } finally {
              br.close();
            }
            String queryProxyResult = resBuffer.toString();
            LOG.info("queryProxyResult: " + queryProxyResult);
            BulkResponse bulkResponse = bulkRequest.execute().actionGet();
            if (bulkResponse.hasFailures()) {
              LOG.error(bulkResponse.buildFailureMessage());
            }
            return (Void) null;
          } catch (Exception e) {
            e.printStackTrace();
          } finally {
            method.releaseConnection();
            client.close();
          }
        }
        return (Void) null;
      }
    });

    jssc.start();
    jssc.awaitTermination();
  }
}
