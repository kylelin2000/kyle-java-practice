package idv.kyle.practice.storm.bolt;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class CallQueryProxyByAsync2Bolt extends BaseRichBolt {
  private static final Logger LOG = LoggerFactory
      .getLogger(CallQueryProxyByAsync2Bolt.class);

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
