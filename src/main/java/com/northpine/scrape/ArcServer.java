package com.northpine.scrape;

import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import static com.northpine.scrape.ArcConstants.ARCGIS_REST_SERVICES;
import static com.northpine.scrape.HttpRequester.Q;

public class ArcServer {



  private static final Logger log = LoggerFactory.getLogger(ArcServer.class);

  private final ConcurrentLinkedQueue<URI> endpoints;


  public ArcServer(URI uri) throws Exception {
    endpoints = new ConcurrentLinkedQueue<>();
    if(checkIfEndpoint(uri)) {
      endpoints.add(uri);
      return;
    }
    if(isValidArcServer(uri)) {
      URI newUri = new URI(uri.toString() + ArcConstants.FORMAT_JSON);
      Q.submitRequest(newUri, (response) -> traverseServer(newUri, response), log::error);
    }
  }

  @SuppressWarnings("ResultOfMethodCallIgnored")
  private boolean checkIfEndpoint(URI uri) {
    String[] paths = uri.getPath().split("/");
    try {
      Integer.parseInt(paths[paths.length - 1]);
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  private boolean isValidArcServer(URI uri) {
    return uri.getPath().contains(ARCGIS_REST_SERVICES);
  }

  public List<URI> getUris() {
    return new ArrayList<>(endpoints);
  }

  private void traverseServer(URI prevUri, String response) {
    if(response == null) {
      return;
    }
    try {

      JSONObject obj = new JSONObject(response);
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
          Q.submitRequest(newUri, (body) -> traverseServer(newUri, body), log::error);
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
          Q.submitRequest(newUri, (body) -> traverseServer(newUri, body), log::error);
        }
      }
      if(layers != null) {
        for(int i = 0; i < layers.length(); i++) {
          JSONObject layer = layers.getJSONObject(i);
          int id = layer.getInt(ArcConstants.ID);

          String newUrlStr = String.format("%s://%s%s", prevUri.getScheme(), prevUri.getHost(), prevUri.getPath());

          URI newUri = new URI(newUrlStr + "/" + id + ArcConstants.FORMAT_JSON);

          Q.submitRequest(newUri, (body) -> traverseServer(newUri, body), log::error);

          endpoints.add(newUri);
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
          Q.submitRequest(newUri, (body) -> traverseServer(newUri, body), log::error);
        }
      }
    } catch (Exception e) {
      log.error("Failed to traverse", e);
    }

  }





  public static void main(String[] args) throws Exception {
    long time = System.currentTimeMillis();
    ArcServer server = new ArcServer(new URI("https://gis.cityoftacoma.org/arcgis/rest/services"));
    Thread.sleep(2000);
    while(Q.numRemaining() > 0) {
      Thread.sleep(100);
    }
    Q.client.close();
    server.getUris().stream()
        .map(uri -> uri.getScheme() + "://" + uri.getHost() + uri.getPath())
        .forEach(log::info);
    long timeTook = System.currentTimeMillis() - time;
    log.info(Long.toString(timeTook / server.getUris().size()) + "ms per layer");
  }




}
