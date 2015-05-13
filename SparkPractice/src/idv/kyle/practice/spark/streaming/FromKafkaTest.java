package idv.kyle.practice.spark.streaming;

import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import kafka.serializer.StringDecoder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaStreamingContextFactory;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Usage: ./bin/spark-submit --class
 * idv.kyle.practice.spark.streaming.JavaKafkaWordCount
 * /root/StormPractice-0.0.1-jar-with-dependencies.jar config.properties
 * 
 */

public class FromKafkaTest {
  private static final Logger LOG = LoggerFactory
      .getLogger(FromKafkaTest.class);
  static String propertiesFileName = "sparkPractice.properties";

  public static void main(String[] args) throws Exception {
    Properties prop = new Properties();
    Path pt = new Path(propertiesFileName);
    FileSystem fs = FileSystem.get(new Configuration());
    prop.load(new InputStreamReader(fs.open(pt)));

    String zkHosts = prop.getProperty("zookeeper.host");
    String kafkaGroup = prop.getProperty("kafka.group");
    String kafkaTopics = prop.getProperty("kafka.topics");
    String threadNumber = prop.getProperty("spark.kafka.thread.num");
    final String esIndex = prop.getProperty("es.index");
    final String esNodes = prop.getProperty("es.nodes");
    String walEnabled = prop.getProperty("spark.WAL.enabled");
    String checkpointEnabled = prop.getProperty("spark.checkpoint.enabled");
    final String checkpointDirectory =
        prop.getProperty("spark.checkpoint.directory");
    String reqAcks = prop.getProperty("kafka.request.required.acks");
    final int batchDuration =
        Integer.parseInt(prop.getProperty("spark.stream.batch.duration.ms"));
    
    LOG.info("read properties: zkHosts=" + zkHosts + ", kafkaTopics=" + kafkaTopics + ", esIndex=" + esIndex);

    JavaStreamingContext jssc;

    if ("true".equals(walEnabled)) {
      LOG.info("write WAL log");

      JavaStreamingContextFactory contextFactory =
          new JavaStreamingContextFactory() {
            @Override
            public JavaStreamingContext create() {
              SparkConf sparkConf =
                  new SparkConf().setAppName("FromKafkaToES-Sync12");
              sparkConf.set("es.index.auto.create", "true");
              sparkConf.set("es.nodes", esNodes);
              sparkConf.set("spark.streaming.receiver.writeAheadLog.enable",
                  "true");
              LOG.info("es.index.auto.create set to "
                  + sparkConf.get("es.index.auto.create")
                  + ", batchDuration is " + batchDuration);

              JavaStreamingContext jssc =
                  new JavaStreamingContext(sparkConf, new Duration(
                      batchDuration));
              jssc.checkpoint(checkpointDirectory);
              return jssc;
            }
          };

      jssc =
          JavaStreamingContext.getOrCreate(checkpointDirectory, contextFactory);
    } else {
      SparkConf sparkConf = new SparkConf().setAppName("FromKafkaToES-Sync");
      sparkConf.set("es.index.auto.create", "true");
      sparkConf.set("es.nodes", esNodes);
      LOG.info("es.index.auto.create set to "
          + sparkConf.get("es.index.auto.create") + ", batchDuration is "
          + batchDuration);
      jssc = new JavaStreamingContext(sparkConf, new Duration(batchDuration));
    }

    if ("true".equals(checkpointEnabled) || "true".equals(walEnabled)) {
      LOG.info("enable checkpoint to directory " + checkpointDirectory);
      jssc.checkpoint(checkpointDirectory);
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
    kafkaParams.put("request.required.acks", reqAcks);

    JavaPairReceiverInputDStream<String, String> messages =
        KafkaUtils.createStream(jssc, String.class, String.class,
            StringDecoder.class, StringDecoder.class, kafkaParams, topicMap,
            StorageLevel.MEMORY_AND_DISK_SER_2());

    messages.print();

    jssc.start();
    jssc.awaitTermination();
  }
}
