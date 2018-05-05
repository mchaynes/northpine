package com.northpine.scrape;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import com.northpine.scrape.ogr.GeoCollector;
import com.northpine.scrape.ogr.OgrCollector;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.CopyOption;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.northpine.scrape.JobManager.MAN;
import static com.northpine.scrape.request.HttpRequester.Q;
import static java.lang.String.format;
import static java.util.concurrent.CompletableFuture.runAsync;

/**
 * Scrapes ArcGIS REST Servers
 */
public class ScrapeJob {

  private static final int CHUNK_SIZE = 200;



  private static final String OUTPUT_FOLDER = "output";



  private static final Logger log = LoggerFactory.getLogger( ScrapeJob.class );

  private final Queue<String> files;

  private final ExecutorService executor;

  private String layerName;

  private AtomicInteger current;

  private AtomicInteger done;

  private AtomicInteger total;

  private AtomicBoolean failed;

  private boolean isDone;

  private String outputFileBase;

  private String outputZip;

  private String layerUrl;

  private String queryUrlStr;

  private String failMessage;

  private File zipFile;


  /**
   * @param layerUrl Does not include "/query" appended to end of url to layer.
   */
  public ScrapeJob(String layerUrl) {
    executor = Executors.newWorkStealingPool();
    current = new AtomicInteger();
    total = new AtomicInteger();
    failed = new AtomicBoolean( false);
    this.layerUrl = layerUrl ;
    this.queryUrlStr = layerUrl + "/query";
    this.layerName = getLayerName();
    this.outputFileBase =  OUTPUT_FOLDER + "/" + layerName;
    this.outputZip =  OUTPUT_FOLDER + "/" + layerName + ".zip";
    files = new PriorityQueue<>();
  }

  public void startScraping() {
    current = new AtomicInteger();
    done = new AtomicInteger();
    isDone = false;
    URL queryUrl = getURL( queryUrlStr + "?where=1=1&returnIdsOnly=true&f=json&outSR=3857" );
    JSONObject idsJson = Q.submitSyncRequest(queryUrl.toString())
        .orElseThrow(RuntimeException::new);
    JSONArray arr = idsJson.getJSONArray( "objectIds" );
    OgrCollector collector = new GeoCollector(outputFileBase);


    buildIdStrs( arr ).stream()
        .map( idListStr -> "OBJECTID in (" + idListStr + ")" )
        .forEach(whereClause -> {
          try {
            String file = outputFileBase + current.incrementAndGet() + ".json";
            var before = System.currentTimeMillis();
            var request = Unirest.get(queryUrlStr)
                .queryString("where", whereClause)
                .queryString("outFields", "*")
                .queryString("f", "json")
                .asBinary();
            log.info(String.format("Request took %dms", System.currentTimeMillis() - before));
            if(request.getStatus() == 200) {
              before = System.currentTimeMillis();
              writeToFile(request.getRawBody(), file);
              files.add(file);
              log.info(String.format("Writing data to file took %dms", System.currentTimeMillis() - before));
              var numDone = done.incrementAndGet();
              var numTotal = total.get();
              if(numDone % (numTotal / 10) == 0) {
                log.info(format("%d/%d requests done for %s", numDone, numTotal, layerUrl));
              }
              Thread.sleep(100);
            } else {
              throw new RuntimeException(request.getStatusText());
            }
          } catch (UnirestException | InterruptedException httpException) {
            log.error("Couldn't connect to server", httpException);
            failJob("Connecting to server for query failed");
          }
        });
    files.forEach(collector::addJsonToPool);
    zipFile = collector.zipUpPool();
    isDone = true;
    log.info("Zipped '" + outputZip + "'");
    log.info("Done with job.");
  }




  public int getNumDone() {
    if(done != null) {
      return done.get();
    } else {
      return -1;
    }
  }

  public void stopJob() {
    executor.shutdownNow();
  }

  public String getName() {
    return layerName;
  }

  public int getTotal() {
    return total.get();
  }

  public boolean isJobDone() {
    return isDone;
  }

  public boolean isFailed() {
    return failed.get();
  }

  public String getFailMessage() {
    return failMessage;
  }

  private void failJob(String failMessage) {
    this.failMessage = failMessage;
    failed.set( true );
  }
  private void writeToFile(InputStream in, String file) {
    try {
      Files.copy(in, Paths.get(file), StandardCopyOption.REPLACE_EXISTING);
    } catch(IOException io) {
      log.error("Failed to write file" + file, io);
      failJob("Failed to write file");
    }
  }

  private String getLayerName() {

    String jsonDetailsUrl = layerUrl + "?f=json";

    return Q.submitSyncRequest(jsonDetailsUrl)
        .orElseThrow( RuntimeException::new )
        .getString( "name" );
  }

  public String getOutput() {
    if ( isJobDone() ) {
      return zipFile.getAbsolutePath();
    } else {
      return null;
    }
  }

  private URL getURL(String str) {
    try {
      return new URL( str );
    } catch ( MalformedURLException e ) {
      throw new IllegalArgumentException( "Query str is invalid '" + str + "'" );
    } catch ( Exception e ) {
      throw new IllegalArgumentException( "Just kill me now" );
    }
  }


  private List<String> buildIdStrs(JSONArray arr) {

    List<String> idChunks = new ArrayList<>();
    int counter = 0;
    StringBuilder sb = new StringBuilder();
    boolean newRow = true;
    //Probably a string
    for ( Object id : arr ) {
      if ( counter % CHUNK_SIZE == 0 && counter != 0 ) {
        total.incrementAndGet();
        sb.append( "," ).append( id );
        idChunks.add( sb.toString() );
        sb = new StringBuilder();
        newRow = true;
      } else if ( newRow ) {
        sb.append( id );
        newRow = false;
      } else {
        sb.append( "," ).append( id );
      }
      counter++;
    }
    return idChunks;
  }
}
