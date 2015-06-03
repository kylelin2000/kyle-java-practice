package idv.kyle.practice.spark.streaming;

import idv.kyle.practice.spark.streaming.streams.ParseQueryProxyResponse;
import idv.kyle.practice.spark.streaming.streams.ReadLines;
import idv.kyle.practice.spark.streaming.streams.ReduceLines;
import idv.kyle.practice.spark.streaming.streams.RequestQueryProxyAsync2;
import idv.kyle.practice.spark.streaming.streams.ResponseFromQueryProxy;
import idv.kyle.practice.spark.streaming.streams.WriteResultToES;

import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;

import kafka.serializer.StringDecoder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.asynchttpclient.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Usage: ./bin/spark-submit --class
 * idv.kyle.practice.spark.streaming.JavaKafkaWordCount
 * /root/StormPractice-0.0.1-jar-with-dependencies.jar config.properties
 * 
 */

public class FromKafkaToESAsyncReduce12 {
  private static final Logger LOG = LoggerFactory
      .getLogger(FromKafkaToESAsyncReduce12.class);

  public static void main(String[] args) throws Exception {
    String esNodes = "";
    String threadNumber = "";
    String kafkaTopics = "";
    String zkHosts = "";
    String kafkaGroup = "";

    Properties prop = new Properties();
    Path pt = new Path(ConstantUtil.propertiesFileName);
    FileSystem fs = FileSystem.get(new Configuration());
    prop.load(new InputStreamReader(fs.open(pt)));
    zkHosts = prop.getProperty("zookeeper.host");
    kafkaGroup = prop.getProperty("kafka.group");
    kafkaTopics = prop.getProperty("kafka.topics");
    threadNumber = prop.getProperty("spark.kafka.thread.num");
    esNodes = prop.getProperty("es.nodes");
    String esIndex = prop.getProperty("es.index");

    LOG.info("read properties: zkHosts=" + zkHosts + ", kafkaTopics="
        + kafkaTopics + ", esIndex=" + esIndex);

    SparkConf sparkConf =
        new SparkConf().setAppName("FromKafkaToES-AsyncReduce12NoWAL");
    sparkConf.set("es.index.auto.create", "true");
    sparkConf.set("es.nodes", esNodes);
    JavaStreamingContext jssc =
        new JavaStreamingContext(sparkConf, new Duration(2000));

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

    JavaDStream<String> lines = messages.map(new ReadLines());

    JavaDStream<String> streams = lines.reduce(new ReduceLines());

    //JavaDStream<String> queryResults = streams.flatMap(new RequestQueryProxyAsync());

    JavaDStream<Future<Response>> queryResults =
        streams.map(new RequestQueryProxyAsync2());

    JavaDStream<String> parseResults =
        queryResults.flatMap(new ParseQueryProxyResponse());

    JavaDStream<Map<String, String>> queryResult =
        parseResults.map(new ResponseFromQueryProxy());

    queryResult.foreach(new WriteResultToES());

    jssc.start();
    jssc.awaitTermination();
  }
}
