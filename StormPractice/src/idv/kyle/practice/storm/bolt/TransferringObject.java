package idv.kyle.practice.storm.bolt;

import org.apache.http.HttpResponse;

import backtype.storm.tuple.Tuple;

public class TransferringObject {
  private HttpResponse response;
  private Tuple tuple;

  public HttpResponse getResponse() {
    return response;
  }

  public void setResponse(HttpResponse response) {
    this.response = response;
  }

  public Tuple getTuple() {
    return tuple;
  }

  public void setTuple(Tuple tuple) {
    this.tuple = tuple;
  }
}
