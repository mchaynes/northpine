package com.northpine;

import com.google.gson.*;
import com.google.gson.stream.JsonWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

public class ArcJsonWriter {

  private static final Logger log = LoggerFactory.getLogger(ArcJsonWriter.class);
  private static final String FEATURES = "features";
  private boolean firstWrite;

  private final JsonWriter writer;


  public ArcJsonWriter(Writer output) {
    writer = new JsonWriter(output);
    firstWrite = true;
  }

  /**
   * This method is used as the public interface to the json writer.
   * If this is the first call to write, we want to be able to write all the associated
   * metadata that is included with a call to an ArcGIS Server. However, we want to keep an
   * open array for future 'features' to be put into the array.
   * @param obj object containing geometric information
   * @throws IOException if failed to write to file
   */
  public void write(JsonObject obj) throws IOException {
    long before = System.currentTimeMillis();
    if(firstWrite) {
      firstWrite = false;
      var features = obj.getAsJsonArray(FEATURES);
      obj.remove(FEATURES);
      writeJsonObject(obj, false);
      if(features != null) {
        writer.name(FEATURES);
        writer.beginArray();
        log.info("Beginning features array");
        for(var i = 0; i < features.size(); i++) {
          log.info("" + i);
          var el = features.get(i);
          checkTypeAndWrite(el);
        }
      } else {
        log.warn("Features is null");
      }
    } else {
      var featuresArr = obj.getAsJsonArray(FEATURES);
      for(var i = 0; i < featuresArr.size(); i++) {
        checkTypeAndWrite(featuresArr.get(i));
      }
    }
    log.info(String.format("Appended json in '%d'ms", System.currentTimeMillis() - before));
  }

  public void close() throws IOException {
    writer.endArray();
    writer.endObject();
    writer.close();
  }

  private void writePrimitive(JsonPrimitive el) throws IOException {
    if(el.isBoolean()) {
//      log.info("Writing boolean: " + el.getAsBoolean());
      writer.value(el.getAsBoolean());
    } else if(el.isNumber()) {
//      log.info("Writing number: " + el.getAsNumber());
      writer.value(el.getAsNumber());
    } else if(el.isString()) {
//      log.info("Writing string: " + el.getAsString());
      writer.value(el.getAsString());
    }
  }

  private void writeJsonObject(JsonObject el) throws IOException {
    writeJsonObject(el, true);
  }
  private void writeJsonObject(JsonObject el, boolean shouldClose) throws IOException {
    writer.beginObject();
    for(var entry: el.entrySet()) {
      writer.name(entry.getKey());
      var val = entry.getValue();
      checkTypeAndWrite(val);
    }
    if(shouldClose) writer.endObject();
  }

  private void checkTypeAndWrite(JsonElement val) throws IOException {
    if(val.isJsonNull()) {
      writer.nullValue();
    } else if(val.isJsonPrimitive()) {
//      log.info("Found primitive: " + val.getAsJsonPrimitive());
      writePrimitive(val.getAsJsonPrimitive());
    } else if(val.isJsonArray()) {
//      log.info("Found array: " + val.getAsJsonArray());
      writeJsonArray(val.getAsJsonArray());
    } else if(val.isJsonObject()) {
//      log.info("Found object: " + val.getAsJsonObject());
      writeJsonObject(val.getAsJsonObject());
    }
  }

  private void writeJsonArray(JsonArray arr) throws IOException {
    writer.beginArray();
    for(var i = 0; i < arr.size(); i++) {
      var el = arr.get(i);
//      log.info("Writing array element: " + i);
      checkTypeAndWrite(el);
    }
    if(true) writer.endArray();

  }








}
