package com.northpine.scrape.request;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.async.Callback;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public enum HttpRequester {

  Q;

  private static final Logger log = LoggerFactory.getLogger(HttpRequester.class);

  HttpRequester() {
    Unirest.setConcurrency(100, 5);
    //Set super long timeout because we're
    Unirest.setTimeouts(100000000, 100000000);

  }

  public CompletableFuture<JSONObject> submitRequest(String uri) {
    HttpCallback callback = new HttpCallback();
    Unirest.get(uri)
        .asJsonAsync(callback);
    return callback;
  }

  public Optional<JSONObject> submitSyncRequest(String uri) {
    try {
      return Optional.of(Unirest.get(uri)
          .asJson()
          .getBody()
          .getObject());
    } catch(UnirestException e) {
      log.error("failed to get: " + uri);
      return Optional.empty();
    }
  }


  private class HttpCallback extends CompletableFuture<JSONObject> implements Callback<JsonNode> {

    @Override
    public void completed(HttpResponse<JsonNode> httpResponse) {
      complete(httpResponse.getBody().getObject());
    }

    @Override
    public void failed(UnirestException e) {
      log.error("failed, and here's how", e);
      completeExceptionally(e);
    }

    @Override
    public void cancelled() {
      completeExceptionally(new UnirestException("cancelled"));
    }
  }



}

