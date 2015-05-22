package idv.kyle.practice.spark.streaming.streams;

import org.apache.spark.api.java.function.Function2;

public class ReduceLines implements Function2<String, String, String> {
  private static final long serialVersionUID = -677330560222778158L;

  @Override
  public String call(String str1, String str2) {
    return str1 + "\n" + str2;
  }
}
