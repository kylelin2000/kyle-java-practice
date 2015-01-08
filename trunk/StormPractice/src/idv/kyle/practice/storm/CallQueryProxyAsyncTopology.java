package idv.kyle.practice.storm;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.CharBuffer;
import java.util.Map;
import java.util.concurrent.Future;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
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

public class CallQueryProxyAsyncTopology {
  public static class HDFSFileReaderSpout extends BaseRichSpout {
    SpoutOutputCollector _collector;
    String _uri;

    public HDFSFileReaderSpout(String uri) {
      _uri = uri;
    }

    @Override
    public void open(Map conf, TopologyContext context,
        SpoutOutputCollector collector) {
      _collector = collector;
    }

    @Override
    public void nextTuple() {
      InputStream in = null;
      try {
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        FileSystem fs = FileSystem.get(URI.create(_uri), conf);
        System.out.println("fs: " + fs);
        in = fs.open(new Path(_uri));
        // IOUtils.copyBytes(in, System.out, 4096, false);
        BufferedReader br = new BufferedReader(new InputStreamReader(in));
        String line = null;
        while (null != (line = br.readLine())) {
          _collector.emit(new Values(line));
          Utils.sleep(100);
        }
      } catch (Exception e) {
        e.printStackTrace();
      } finally {
        IOUtils.closeStream(in);
      }
    }

    @Override
    public void ack(Object id) {
    }

    @Override
    public void fail(Object id) {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("url"));
    }
  }

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
        while (buf.hasRemaining()) {
          String inputLine = buf.toString();
          LOG.info("result from query proxy : " + inputLine);
          try {
            JSONObject jsonObj = new JSONObject(inputLine);
            if ("200".equals(jsonObj.get("status").toString())) {
              _collector.emit(new Values(jsonObj.get("status").toString(),
                  jsonObj.get("result").toString(), jsonObj.get("service")
                      .toString()));
            }
          } catch (Exception e) {
            e.printStackTrace();
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

  public static void main(String[] args) throws Exception {
    if (args == null || args.length != 3) {
      System.out
          .println("Wrong number of arguments!!!"
              + "arg1: topology_name(Ex:TP), arg2: HDFS file path(Ex:hdfs://192.168.1.10:8020/tmp/maillog), arg3: ES index/type(Ex:stormtest/docs)");
    } else {
      TopologyBuilder builder = new TopologyBuilder();

      builder.setSpout("spout", new HDFSFileReaderSpout(args[1]), 5);

      builder.setBolt("query", new CallQueryProxy(), 8)
          .shuffleGrouping("spout");
      builder.setBolt("es-bolt", new EsBolt(args[2]), 5).shuffleGrouping(
          "query");

      Config conf = new Config();
      conf.setDebug(true);
      conf.put("es.index.auto.create", "true");

      conf.setNumWorkers(2);

      StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
    }
  }
}
