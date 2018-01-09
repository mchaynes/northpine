package com.northpine.scrape.arc;

import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static com.northpine.scrape.arc.ArcConstants.NAME;
import static com.northpine.scrape.request.HttpRequester.Q;
import static java.util.concurrent.Executors.newFixedThreadPool;

public class ArcServer {



  private static final Logger log = LoggerFactory.getLogger(ArcServer.class);

  private final ConcurrentLinkedQueue<ArcLayer> endpoints;

  private volatile String lastCall;

  private final AtomicInteger remaining;


  public ArcServer(URI uri) throws Exception {
    log.info("Constructor called");
    remaining = new AtomicInteger();
    endpoints = new ConcurrentLinkedQueue<>();

      URI newUri = new URI(uri.toString() + ArcConstants.FORMAT_JSON);
      Q.submitRequest(newUri.toString())
          .thenAccept((response) -> traverseServer(newUri, response));

      newFixedThreadPool(1).submit(this::provideUpdates);

  }

  public boolean isDone() {
    return remaining.get() == 0;
  }

  private void provideUpdates() {
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      log.error("Notifier died", e);
    }
    if(isDone()) {
      log.info("Finished");
    } else {
      log.info(lastCall);
      provideUpdates();
    }
  }


  private void traverseServer(URI prevUri, JSONObject obj) {
    lastCall = prevUri.toString();
    if(obj == null) {
      return;
    }
    try {
      if(obj.has(NAME)) {
        endpoints.add(new ArcLayer(prevUri.toString(), obj.getString(NAME)));
        return;
      }
      JSONArray folders = obj.optJSONArray(ArcConstants.FOLDERS);
      JSONArray services = obj.optJSONArray(ArcConstants.SERVICES);
      JSONArray layers = obj.optJSONArray(ArcConstants.LAYERS);
      JSONArray subLayers = obj.optJSONArray(ArcConstants.SUB_LAYERS);
      if (folders != null) {
        for (Object folder : folders) {
          String folderName = folder.toString();
          String newUrl = String.format("%s://%s%s/%s%s", prevUri.getScheme(),
              prevUri.getHost(), prevUri.getPath(), folderName, ArcConstants.FORMAT_JSON);
          URI newUri = new URI(newUrl);
          remaining.incrementAndGet();
          Q.submitRequest(newUri.toString())
              .thenAccept((body) -> traverseAndDecrement(newUri, body));
        }
      }
      if(services != null) {
        for(int i = 0; i < services.length(); i++) {
          JSONObject serviceDef = services.getJSONObject(i);
          String type = serviceDef.getString("type");
          String serviceName = serviceDef.getString("name");
          String newUriStr = String.format("%s://%s%s/", prevUri.getScheme(), prevUri.getHost(), prevUri.getPath());
          if(serviceName.contains("/")) {
            serviceName = serviceName.split("/")[1];
          }
          newUriStr += serviceName + "/" + type + ArcConstants.FORMAT_JSON;
          URI newUri = new URI(newUriStr);
          remaining.incrementAndGet();
          Q.submitRequest(newUri.toString())
              .thenAccept( (body) -> traverseAndDecrement(newUri, body));
        }
      }
      if(layers != null) {
        for(int i = 0; i < layers.length(); i++) {
          JSONObject layer = layers.getJSONObject(i);
          int id = layer.getInt(ArcConstants.ID);
          String newUrlStr = String.format("%s://%s%s", prevUri.getScheme(), prevUri.getHost(), prevUri.getPath());
          URI newUri = new URI(newUrlStr + "/" + id + ArcConstants.FORMAT_JSON);
          remaining.incrementAndGet();
          Q.submitRequest(newUri.toString())
              .thenAccept( (body) ->  traverseAndDecrement(newUri, body));
        }
      }
      if(subLayers != null) {
        for(int i = 0; i < subLayers.length(); i++) {
          JSONObject layer = subLayers.getJSONObject(i);
          int id = layer.getInt("id");
          String[] splitPath = prevUri.getPath().split("/");
          StringBuilder newPath = new StringBuilder();
          for(int j = 0; j < splitPath.length - 1; j++) {
            newPath.append(splitPath[j]);
            if(j < splitPath.length -2) {
              newPath.append("/");
            }
          }
          String newUrlStr = String.format("%s://%s%s/%s%s", prevUri.getScheme(),
              prevUri.getHost(), newPath.toString(), id, ArcConstants.FORMAT_JSON);
          URI newUri = new URI(newUrlStr);
          remaining.incrementAndGet();
          Q.submitRequest(newUri.toString())
              .thenAccept((body) -> traverseAndDecrement(newUri, body));
        }
      }
    } catch (Exception e) {
      log.error("Failed to traverse", e);
    }
  }

  private void traverseAndDecrement(URI uri, JSONObject body) {
    remaining.decrementAndGet();
    traverseServer(uri, body);
  }

  public List<ArcLayer> getLayers() {
    return new ArrayList<>(endpoints);
  }





}
