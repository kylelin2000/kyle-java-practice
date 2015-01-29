package idv.kyle.practice.storm.bolt;

import java.io.IOException;
import java.nio.CharBuffer;
import java.util.Map;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.nio.IOControl;
import org.apache.http.nio.client.methods.AsyncCharConsumer;
import org.apache.http.nio.client.methods.HttpAsyncMethods;
import org.apache.http.nio.protocol.HttpAsyncRequestProducer;
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

public class CallQueryProxyByAsyncBolt extends BaseRichBolt {
  private static final Logger LOG = LoggerFactory
      .getLogger(CallQueryProxyByAsyncBolt.class);

  private CloseableHttpAsyncClient _httpclient;
  OutputCollector _collector;
  Tuple _tuple;

  @Override
  public void prepare(Map conf, TopologyContext context,
      OutputCollector collector) {
    _collector = collector;
    _httpclient = HttpAsyncClients.createDefault();
    _httpclient.start();
  }

  @Override
  public void execute(Tuple tuple) {
    _tuple = tuple;
    Utils.sleep(100);
    Thread t = Thread.currentThread();
    LOG.info("Thread name: " + t.getName() + ", Thread id: " + t.getId());
    String url = tuple.getString(0);
    LOG.info("query proxy url : " + url);
    sendAsyncGetRequest(url);
    LOG.info("execute finished");
  }

  private void sendAsyncGetRequest(String url) {
    HttpGet request = new HttpGet(url);
    HttpAsyncRequestProducer producer = HttpAsyncMethods.create(request);
    AsyncCharConsumer<HttpResponse> consumer =
        new AsyncCharConsumer<HttpResponse>() {
          HttpResponse response;

          @Override
          protected void onResponseReceived(final HttpResponse response) {
            LOG.info("into func onResponseReceived");
            this.response = response;
          }

          @Override
          protected void onCharReceived(final CharBuffer buf,
              final IOControl ioctrl) throws IOException {
            LOG.info("into func onCharReceived");
            String inputLine;
            char[] bufResult = new char[buf.remaining()];
            int idx = 0;
            while (buf.hasRemaining()) {
              bufResult[idx++] = buf.get();
            }
            inputLine = new String(bufResult);

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
            _collector.ack(_tuple);
          }

          @Override
          protected void releaseResources() {
            LOG.info("into func releaseResources");
          }

          @Override
          protected HttpResponse buildResult(final HttpContext context) {
            LOG.info("into func buildResult");
            return this.response;
          }
        };
    _httpclient.execute(producer, consumer, new FutureCallback<HttpResponse>() {

      @Override
      public void completed(HttpResponse response) {
        LOG.info("callback response :" + response.toString());
      }

      @Override
      public void failed(Exception ex) {
        throw new RuntimeException(ex);
      }

      @Override
      public void cancelled() {
        LOG.warn("Async http request canceled!");
      }
    });
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("status", "result", "service"));
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    return null;
  }
}
