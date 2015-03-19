package idv.kyle.practice.kafka;

import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Properties;
import java.util.Random;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class QueryProxyUrlProducer {
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
    if (args == null || args.length != 3) {
      System.out.println("Wrong number of arguments!!!"
          + "arg1: broker_list, arg2: number of records, arg3: topic_name");
    } else {
      long events = Long.parseLong(args[1]);
      Random rnd = new Random();

      Properties props = new Properties();
      props.put("metadata.broker.list", args[0]);
      props.put("serializer.class", "kafka.serializer.StringEncoder");
      props.put("partitioner.class",
          "idv.kyle.practice.kafka.SimplePartitioner");
      props.put("request.required.acks", "1");

      ProducerConfig config = new ProducerConfig(props);

      Producer<String, String> producer = new Producer<String, String>(config);
      String[] engines = { "VT", "GRID", "APTKB", "CENSUS", "DIG" };
      String[] urls =
          {
              "https://issues.apache.org/jira/browse/SPARK-4062",
              "https://databricks.com/blog/2014/12/08/pearson-uses-spark-streaming-for-next-generation-adaptive-learning-platform.html",
              "http://stackoverflow.com/questions/6851909/how-do-i-delete-everything-in-redis" };
      SecureRandom prng = SecureRandom.getInstance("SHA1PRNG");

      for (long nEvents = 0; nEvents < events; nEvents++) {
        String randomNum = new Integer(prng.nextInt()).toString();
        MessageDigest sha = MessageDigest.getInstance("SHA-1");
        String currentEngine = engines[rnd.nextInt(engines.length)];
        String url = "";
        if ("DIG".equals(currentEngine)) {
          url =
              "http://10.1.193.226:9090/v1/" + currentEngine + "/_tag?q=URL:"
                  + urls[rnd.nextInt(urls.length)] + "&useCache=false";
        } else {
          byte[] result = sha.digest(randomNum.getBytes());
          url =
              "http://10.1.193.226:9090/v1/" + currentEngine + "/_tag?q=HASH:"
                  + hexEncode(result) + "&useCache=false";
        }
        KeyedMessage<String, String> data =
            new KeyedMessage<String, String>(args[2], url);
        producer.send(data);
      }
      producer.close();
    }
  }
}
