package com.northpine.scrape.request;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.async.Callback;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.function.Consumer;

public enum HttpRequester {

  Q;

  private static final Logger log = LoggerFactory.getLogger(HttpRequester.class);

  HttpRequester() {
    Unirest.setConcurrency(1000, 100);

  }


  public void submitRequest(URI uri, Consumer<JSONObject> onSuccess, Consumer<String> onFailure) {
    Unirest.get(uri.toString())
        .asJsonAsync(new HttpCallback(onSuccess, onFailure));
  }


  private class HttpCallback implements Callback<JsonNode> {

    private Consumer<JSONObject> onSuccess;

    private Consumer<String> onFailure;

    HttpCallback(Consumer<JSONObject> onSuccess, Consumer<String> onFailure) {
      this.onSuccess = onSuccess;
      this.onFailure = onFailure;
    }


    @Override
    public void completed(HttpResponse<JsonNode> httpResponse) {
      onSuccess.accept(httpResponse.getBody().getObject());
    }

    @Override
    public void failed(UnirestException e) {
      log.error("failed, and here's how", e);
      onFailure.accept(e.getMessage());
    }

    @Override
    public void cancelled() {
      onFailure.accept("cancelled");
    }
  }



}

