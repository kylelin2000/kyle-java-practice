package idv.kyle.practice.spark.streaming.streams;

import idv.kyle.practice.spark.streaming.ConstantUtil;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;
import java.util.concurrent.Future;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.function.Function;
import org.asynchttpclient.AsyncCompletionHandler;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.AsyncHttpClientConfig;
import org.asynchttpclient.Response;
import org.asynchttpclient.extras.registry.AsyncHttpClientFactory;
import org.asynchttpclient.providers.netty4.NettyAsyncHttpProviderConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RequestQueryProxyAsync2 implements
    Function<String, Future<Response>> {
  private static final Logger LOG = LoggerFactory
      .getLogger(RequestQueryProxyAsync2.class);
  private static final long serialVersionUID = 7569717442952954475L;

  @Override
  public Future<Response> call(String postBody) {
    LOG.info("query proxy postBody : " + postBody.trim());
    Properties prop = new Properties();
    Path pt = new Path(ConstantUtil.propertiesFileName);
    try {
      FileSystem fs = FileSystem.get(new Configuration());
      prop.load(new InputStreamReader(fs.open(pt)));
    } catch (IOException e1) {
      e1.printStackTrace();
    }
    String queryProxyUrl = prop.getProperty("queryproxy.url.batch");

    NettyAsyncHttpProviderConfig providerConfig =
        new NettyAsyncHttpProviderConfig();
    AsyncHttpClientConfig clientConfig =
        new AsyncHttpClientConfig.Builder().setAllowPoolingConnections(true)
            .setAsyncHttpClientProviderConfig(providerConfig)
            .setMaxConnectionsPerHost(2).setMaxConnections(2).build();
    AsyncHttpClient asyncHttpClient =
        AsyncHttpClientFactory.getAsyncHttpClient(clientConfig);
    return asyncHttpClient.preparePost(queryProxyUrl).setBody(postBody.trim())
        .execute(new AsyncCompletionHandler<Response>() {
          @Override
          public Response onCompleted(Response response) throws Exception {
            LOG.info("async client onCompleted");
            return response;
          }

          @Override
          public void onThrowable(Throwable t) {
            t.printStackTrace();
          }
        });
  }
}
