package idv.kyle.hdfs;

import java.io.IOException;
import java.net.URI;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

public class SequenceFileWriteDemo {

  private static final String[] DATA = { "One, two, buckle my shoe",
      "Three, four, shut the door", "Five, six, pick up sticks",
      "Seven, eight, lay them straight", "Nine, ten, a big fat hen" };

  public static void main(String[] args) throws IOException,
      InterruptedException, LoginException {
    System.setProperty("sun.security.krb5.debug", "true");
    System.setProperty("java.security.krb5.realm", "LOCALDOMAIN");
    System.setProperty("java.security.krb5.kdc", "hdp2-n1.localdomain");
    System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
    System.setProperty("java.security.auth.login.config", "/Users/kyle/jaas.conf");
    LoginContext context =
        new LoginContext("kyle", new KerberosClientCallbackHandler("kyle",
            "novirus"));
    context.login();

    Configuration conf = new Configuration();
    conf.set("fs.defaultFS", "hdfs://10.1.192.76:8020");

    String uri = "/tmp/seq/file1";
    FileSystem fs = FileSystem.get(URI.create(uri), conf);
    Path path = new Path(uri);
    IntWritable key = new IntWritable();
    Text value = new Text();
    SequenceFile.Writer writer = null;
    try {
      writer =
          SequenceFile.createWriter(fs, conf, path, key.getClass(),
              value.getClass());
      for (int i = 0; i < 100; i++) {
        key.set(100 - i);
        value.set(DATA[i % DATA.length]);
        System.out.printf("[% s]\t% s\t% s\n", writer.getLength(), key, value);
        writer.append(key, value);
      }
    } finally {
      IOUtils.closeStream(writer);
    }
  }

  private static class KerberosClientCallbackHandler implements CallbackHandler {
    private String username;
    private String password;

    public KerberosClientCallbackHandler(String username, String password) {
      this.username = username;
      this.password = password;
    }

    public void handle(Callback[] callbacks) throws IOException,
        UnsupportedCallbackException {
      for (Callback callback : callbacks) {
        if (callback instanceof NameCallback) {
          NameCallback ncb = (NameCallback) callback;
          ncb.setName(username);
        } else if (callback instanceof PasswordCallback) {
          PasswordCallback pwcb = (PasswordCallback) callback;
          pwcb.setPassword(password.toCharArray());
        } else {
          throw new UnsupportedCallbackException(callback, "We got a "
              + callback.getClass().getCanonicalName()
              + ", but only NameCallback and PasswordCallback is supported");
        }
      }
    }
  }
}