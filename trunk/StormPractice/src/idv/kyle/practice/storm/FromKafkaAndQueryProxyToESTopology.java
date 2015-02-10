package idv.kyle.practice.storm;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import idv.kyle.practice.storm.bolt.CallQueryProxyByAsync2Bolt;
import idv.kyle.practice.storm.bolt.CallQueryProxyByAsync3Bolt;
import idv.kyle.practice.storm.bolt.CallQueryProxyByAsyncBolt;
import idv.kyle.practice.storm.bolt.CallQueryProxyBySyncBolt;

import java.util.Properties;

import org.elasticsearch.storm.EsBolt;

import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import storm.kafka.trident.TridentKafkaState;

public class FromKafkaAndQueryProxyToESTopology {
  protected static KafkaSpout createKafkaSpout(String topic, String zkRootPath,
      String id, long startOffset) {
    BrokerHosts hosts = new ZkHosts("sparkvm.localdomain:2181", "/brokers");
    SpoutConfig spoutConfig = new SpoutConfig(hosts, topic, zkRootPath, id);
    spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
    spoutConfig.forceFromStart = false;
    // spoutConfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
    // -1: from resent, -2: from beginning
    spoutConfig.startOffsetTime = startOffset;

    KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);

    return kafkaSpout;
  }

  public static void main(String[] args) throws Exception {
    if (args == null || args.length != 10) {
      System.out
          .println("Wrong number of arguments!!!"
              + "arg1: topology_name(Ex:TP), arg2: zk root(Ex:/kafka-storm), arg3: topic_name, arg4: ES index/type(Ex:stormtest/docs), arg5: startOffset, arg6: usedAsyncHttp(Ex.T or F), arg7: worker_number, arg8: spout_thread_num, arg9: bolt_thread_num, arg10: es_bolt_thread_num");
    } else {
      TopologyBuilder builder = new TopologyBuilder();

      builder.setSpout(
          "spout",
          createKafkaSpout(args[2], args[1], "es-consumer",
              Long.parseLong(args[4])), Integer.parseInt(args[7]));
      if ("T".equals(args[5])) {
        builder.setBolt("query", new CallQueryProxyByAsync2Bolt(),
            Integer.parseInt(args[8])).shuffleGrouping("spout");
      } else {
        builder.setBolt("query", new CallQueryProxyBySyncBolt(),
            Integer.parseInt(args[8])).shuffleGrouping("spout");
      }

      builder.setBolt("es-bolt", new EsBolt(args[3], true),
          Integer.parseInt(args[9])).shuffleGrouping("query");

      Config conf = new Config();
      conf.setDebug(true);
      conf.put("es.index.auto.create", "true");
      conf.put("request.required.acks", "1");
      conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, "90");
      conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 5);
      conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 10);
      // conf.put(Config.TOPOLOGY_ACKER_EXECUTORS, 20);

      Properties props = new Properties();
      props.put("metadata.broker.list", "sparkvm.localhost:6667");
      props.put("serializer.class", "kafka.serializer.StringEncoder");
      props.put("request.required.acks", "1");
      props.put("auto.commit.interval.ms", 10 * 1000);
      props.put("consumer.timeout.ms", 15 * 1000);
      conf.put(TridentKafkaState.KAFKA_BROKER_PROPERTIES, props);

      conf.setNumWorkers(Integer.parseInt(args[6]));
      conf.setMessageTimeoutSecs(60);

      StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
    }
  }
}
