package idv.kyle.practice.kafka;

import java.security.MessageDigest;
import java.security.SecureRandom;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class QueryProxyCriteriaProducer {
  private static final Logger LOG = LoggerFactory
      .getLogger(QueryProxyCriteriaProducer.class);
  static private String hexEncode(byte[] aInput) {
    StringBuilder result = new StringBuilder();
    char[] digits =
        { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd',
            'e', 'f' };
    for (int idx = 0; idx < aInput.length; ++idx) {
      byte b = aInput[idx];
      result.append(digits[(b & 0xf0) >> 4]);
      result.append(digits[b & 0x0f]);
    }
    return result.toString();
  }

  public static void main(String[] args) throws Exception {
    if (args == null || args.length != 5) {
      System.out.println("Wrong number of arguments!!!"
              + "arg1: broker_list, arg2: number of records, arg3: topic_name, arg4: batch_size, arg5:request.required.acks");
    } else {
      long events = Long.parseLong(args[1]);
      Random rnd = new Random();

      LOG.info("prepare config of Kafka");
      Properties props = new Properties();
      props.put("metadata.broker.list", args[0]);
      props.put("serializer.class", "kafka.serializer.StringEncoder");
      props.put("partitioner.class",
          "idv.kyle.practice.kafka.SimplePartitioner");
      props.put("request.required.acks", args[4]);

      ProducerConfig config = new ProducerConfig(props);

      Producer<String, String> producer = new Producer<String, String>(config);
      String[] engines = { "VT", "GRID", "APTKB", "CENSUS", "DIG" };
      String[] urls =
          {
              "https://issues.apache.org/jira/browse/SPARK-4062",
              "https://databricks.com/blog/2014/12/08/pearson-uses-spark-streaming-for-next-generation-adaptive-learning-platform.html",
              "http://stackoverflow.com/questions/6851909/how-do-i-delete-everything-in-redis" };

      Date startTime = new Date();
      List<KeyedMessage<String, String>> dataList = new ArrayList<KeyedMessage<String, String>>();
      int batchSize = Integer.parseInt(args[3]);
      LOG.info("start to send messages into Kafka");
      for (long nEvents = 1; nEvents <= events; nEvents++) {
        Random rand = new Random();
        String randomNum = new Integer(rand.nextInt(1000000) + 1).toString();
        MessageDigest sha = MessageDigest.getInstance("SHA-1");
        String currentEngine = engines[rnd.nextInt(engines.length)];
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        int num = rand.nextInt(1000) + 1;
        String context = num + ", send to kafka at " + sdf.format(new Date());
        String criteria = "";
        if ("DIG".equals(currentEngine)) {
          criteria =
              "{\"useCache\":false,\"context\":\"" + context
                  + "\",\"value\":\"" + urls[rnd.nextInt(urls.length)]
                  + "\",\"service\":\"" + currentEngine
                  + "\",\"type\":\"URL\"}";
        } else {
          byte[] result = sha.digest(randomNum.getBytes());
          criteria =
              "{\"useCache\":false,\"context\":\"" + context
                  + "\",\"value\":\"" + hexEncode(result) + "\",\"service\":\""
                  + currentEngine + "\",\"type\":\"HASH\"}";
        }
        KeyedMessage<String, String> data =
            new KeyedMessage<String, String>(args[2], criteria);
        dataList.add(data);
        if (nEvents % batchSize == 0) {
          LOG.info("sent " + batchSize + " messages");
          producer.send(dataList);
          dataList.clear();
        }
      }
      if (dataList.size() > 0) {
        LOG.info("sent " + dataList.size() + " messages");
        producer.send(dataList);
        dataList.clear();
      }
      producer.close();

      Date endTime = new Date();
      LOG.info("totally sent " + events + " messages. Cost time (second): "
          + (endTime.getTime() - startTime.getTime()));
    }
  }
}
