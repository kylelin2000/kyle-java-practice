package idv.kyle.practice.spark.streaming.streams;

import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

public class ReadLines implements Function<Tuple2<String, String>, String> {
  private static final long serialVersionUID = 9064101206599289859L;

  @Override
  public String call(Tuple2<String, String> tuple2) {
    String item2 = tuple2._2();
    return item2;
  }
}
