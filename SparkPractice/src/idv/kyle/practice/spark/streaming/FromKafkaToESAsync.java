package idv.kyle.practice.spark.streaming;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import kafka.serializer.StringDecoder;

import com.ning.http.client.AsyncCompletionHandler;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.AsyncHttpClientConfig;
import com.ning.http.client.Response;

import org.apache.commons.httpclient.HttpStatus;
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
 * /root/StormPractice-0.0.1-jar-with-dependencies.jar localhost:2181
 * kafka-spark-1 tp001 1
 * 
 */

public class FromKafkaToESAsync {
  private static final Logger LOG = LoggerFactory
      .getLogger(FromKafkaToESAsync.class);
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
        }, new Duration(10000), new Duration(10000));

    JavaDStream<String> queryResults =
        streams.flatMap(new FlatMapFunction<String, String>() {
          private static final long serialVersionUID = 6272424972267329328L;

          @Override
          public Iterable<String> call(String postBody) {
            LOG.info("query proxy postBody : " + postBody.trim());

            AsyncHttpClientConfig clientConfig =
                new AsyncHttpClientConfig.Builder()
                    .setAllowPoolingConnections(true)
                    .setMaxConnectionsPerHost(5).setMaxConnections(5).build();
            AsyncHttpClient asyncHttpClient = new AsyncHttpClient(clientConfig);
            Future<Response> future =
                asyncHttpClient
                    .preparePost("http://10.1.192.49:9090/v1/_bulk_tag")
                    .setBody(postBody.trim())
                    .execute(new AsyncCompletionHandler<Response>() {
                      @Override
                      public Response onCompleted(Response response)
                          throws Exception {
                        return response;
                      }

                      @Override
                      public void onThrowable(Throwable t) {
                        t.printStackTrace();
                      }
                    });

            try {
              Response response = future.get();
              int status = response.getStatusCode();
              LOG.info("query proxy response status : " + status);
              if (status == HttpStatus.SC_OK) {
                InputStream resStream = response.getResponseBodyAsStream();
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
              } else {
                LOG.warn("query fail. status : " + status);
              }
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
            LOG.info("should not run here");
            return new ArrayList<String>();
          }
        });

    queryResults.print();

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
