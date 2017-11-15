package com.northpine.scrape;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public enum HttpRequester {

  Q;

  private static final Logger log = LoggerFactory.getLogger(HttpRequester.class);


  public CloseableHttpAsyncClient client;

  private AtomicInteger remaining;

  private HttpClient syncClient;


  HttpRequester() {
    remaining = new AtomicInteger();
    RequestConfig config = RequestConfig.DEFAULT;
    client = HttpAsyncClients.custom()
        .setDefaultRequestConfig(config)
        .build();
    client.start();
    syncClient = HttpClientBuilder.create().build();
  }

  public int numRemaining() {
    return remaining.get();
  }


  public void submitRequest(URI uri, Consumer<String> onSuccess, Consumer<String> onFailure) {
    long time = System.currentTimeMillis();
    HttpGet get = new HttpGet(uri);
    remaining.incrementAndGet();
    FutureCallback<HttpResponse> callback = new HttpCallback(onSuccess, onFailure, time);
    client.execute(get,callback);
  }

  public Response submitSyncRequest(URI uri) {
    Response response = new Response();
    try {
      HttpGet get = new HttpGet(uri);
      HttpResponse httpResponse = syncClient.execute(get);
      String body = body(httpResponse);
      response.setBody(body);
    } catch(IOException io) {
      log.error("error",io);
      response.setError(io.getMessage());
    }
    return response;

  }

  private String body(HttpResponse response) throws IOException {
    Scanner in = new Scanner(response.getEntity().getContent());
    StringBuilder sb = new StringBuilder();
    while(in.hasNext()) sb.append(in.next());
    return sb.toString();
  }




  public class Response {

    private String error;

    private String body;

    public String getBody() { return body; }

    public String getError() { return error; }

    public JSONObject getJSON() throws JSONException {
      //Lazy load json object. no point in parsing if its not necessary.
      if(getBody() != null) return new JSONObject(getBody());
      else return null;
    }
    void setBody(String body) { this.body = body; }

    void setError(String error) { this.error = error; }

  }


  private class HttpCallback implements FutureCallback<HttpResponse> {

    private Consumer<String> onSuccess;

    private long timeStarted;

    private Consumer<String> onFailure;

    HttpCallback(Consumer<String> onSuccess, Consumer<String> onFailure, long timeStarted) {
      this.onSuccess = onSuccess;
      this.onFailure = onFailure;
      this.timeStarted = timeStarted;
    }

    @Override
    public void completed(HttpResponse httpResponse) {
      log.info(Long.toString(System.currentTimeMillis() - timeStarted));
      remaining.decrementAndGet();
      try {
        Scanner in  = new Scanner(httpResponse.getEntity().getContent());
        StringBuilder sb = new StringBuilder();
        while(in.hasNext()) {
          sb.append(in.next());
        }
        onSuccess.accept(sb.toString());

      } catch (IOException e) {
        failed(e);
      }
    }

    @Override
    public void failed(Exception e) {
      log.error("failed, and here's how", e);
      onFailure.accept(e.getMessage());
    }

    @Override
    public void cancelled() {
      onFailure.accept("cancelled");
    }
  }



}

