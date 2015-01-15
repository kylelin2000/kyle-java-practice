package idv.kyle.practice.storm;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.io.IOException;
import java.nio.CharBuffer;
import java.util.Map;
import java.util.concurrent.Future;

import org.apache.http.HttpResponse;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.nio.IOControl;
import org.apache.http.nio.client.methods.AsyncCharConsumer;
import org.apache.http.nio.client.methods.HttpAsyncMethods;
import org.apache.http.protocol.HttpContext;
import org.codehaus.jettison.json.JSONObject;
import org.elasticsearch.storm.EsBolt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

public class FromKafkaAndQueryProxyToESTopology {
  public static class CallQueryProxy extends BaseRichBolt {
    private static final Logger LOG = LoggerFactory
        .getLogger(CallQueryProxy.class);

    CloseableHttpAsyncClient _httpclient;
    OutputCollector _collector;

    @Override
    public void prepare(Map conf, TopologyContext context,
        OutputCollector collector) {
      _collector = collector;
      _httpclient = HttpAsyncClients.createDefault();
    }

    @Override
    public void execute(Tuple tuple) {
      Utils.sleep(100);
      Thread t = Thread.currentThread();
      LOG.info("Thread name: " + t.getName() + ", Thread id: " + t.getId());
      String url = tuple.getString(0);
      LOG.info("query proxy url : " + url);
      try {
        _httpclient.start();
        final Future<Boolean> future =
            _httpclient.execute(HttpAsyncMethods.createGet(url),
                new MyResponseConsumer(), null);
        final Boolean result = future.get();
        if (result != null && result.booleanValue()) {
          LOG.info("Request successfully executed");
        } else {
          LOG.info("Request failed");
        }
      } catch (Exception e) {
        e.printStackTrace();
      } finally {
        try {
          _httpclient.close();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("status", "result", "service"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
      return null;
    }

    class MyResponseConsumer extends AsyncCharConsumer<Boolean> {
      @Override
      protected void onResponseReceived(final HttpResponse response) {
      }

      @Override
      protected void onCharReceived(final CharBuffer buf, final IOControl ioctrl)
          throws IOException {
        String inputLine = "";
        if (buf.hasRemaining()) {
          inputLine = buf.toString();
          LOG.info("in loop to get inputLine : " + inputLine);
        }
        LOG.info("result from query proxy : " + inputLine);
        if (!inputLine.isEmpty()) {
          try {
            JSONObject jsonObj = new JSONObject(inputLine);
            if ("200".equals(jsonObj.get("status").toString())) {
              _collector.emit(new Values(jsonObj.get("status").toString(),
                  jsonObj.get("result").toString(), jsonObj.get("service")
                      .toString()));
            }
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
      }

      @Override
      protected void releaseResources() {
      }

      @Override
      protected Boolean buildResult(final HttpContext context) {
        return Boolean.TRUE;
      }
    }
  }

  protected static KafkaSpout createKafkaSpout(String topic, String zkRootPath,
      String id) {
    BrokerHosts hosts = new ZkHosts("sparkvm.localdomain:2181", "/brokers");
    SpoutConfig spoutConfig = new SpoutConfig(hosts, topic, zkRootPath, id);
    spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
    spoutConfig.forceFromStart = true;
    spoutConfig.startOffsetTime = -1;

    KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);

    return kafkaSpout;
  }

  public static void main(String[] args) throws Exception {
    if (args == null || args.length != 4) {
      System.out
          .println("Wrong number of arguments!!!"
              + "arg1: topology_name(Ex:TP), arg2: zk root(Ex:/kafka-storm), arg3: topic_name, arg4: ES index/type(Ex:stormtest/docs)");
    } else {
      TopologyBuilder builder = new TopologyBuilder();

      builder.setSpout("spout",
          createKafkaSpout(args[2], args[1], "consumer1"), 1);
      builder.setBolt("query", new CallQueryProxy(), 3)
          .shuffleGrouping("spout");
      builder.setBolt("es-bolt", new EsBolt(args[3]), 3).shuffleGrouping(
          "query");

      Config conf = new Config();
      conf.setDebug(true);
      conf.put("es.index.auto.create", "true");

      conf.setNumWorkers(2);

      StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
    }
  }
}
