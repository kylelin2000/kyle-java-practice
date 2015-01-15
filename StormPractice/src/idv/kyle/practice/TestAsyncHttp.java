package idv.kyle.practice;

import java.io.IOException;
import java.nio.CharBuffer;
import java.util.concurrent.Future;

import org.apache.http.HttpResponse;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.nio.IOControl;
import org.apache.http.nio.client.methods.AsyncCharConsumer;
import org.apache.http.nio.client.methods.HttpAsyncMethods;
import org.apache.http.protocol.HttpContext;

public class TestAsyncHttp {
  static class MyResponseConsumer extends AsyncCharConsumer<Boolean> {
    @Override
    protected void onResponseReceived(final HttpResponse response) {
    }

    @Override
    protected void onCharReceived(final CharBuffer buf, final IOControl ioctrl)
        throws IOException {
      // buf.flip();
      if (buf.hasRemaining()) {
        System.out.println(buf.toString());
      }
      // buf.clear();
    }

    @Override
    protected void releaseResources() {
    }

    @Override
    protected Boolean buildResult(final HttpContext context) {
      return Boolean.TRUE;
    }
  }

  public static void main(String[] args) throws Exception {
    String url =
        "http://10.1.193.226:9090/v1/VT/_tag?q=HASH:bff7ec12e2ad0d173178a86db1f06373a5e0b0f7&useCache=false";
    CloseableHttpAsyncClient httpclient = HttpAsyncClients.createDefault();
    try {

      httpclient.start();
      final Future<Boolean> future =
          httpclient.execute(HttpAsyncMethods.createGet(url),
              new MyResponseConsumer(), null);
      final Boolean result = future.get();
      if (result != null && result.booleanValue()) {
        System.out.println("Request successfully executed");
      } else {
        System.out.println("Request failed");
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      httpclient.close();
    }
  }
}
