package com.northpine.scrape;

import com.northpine.scrape.ogr.GeoCollector;
import com.northpine.scrape.ogr.OgrCollector;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.northpine.scrape.JobManager.MAN;
import static com.northpine.scrape.request.HttpRequester.Q;
import static java.util.concurrent.CompletableFuture.runAsync;

/**
 * Scrapes ArcGIS REST Servers
 */
public class ScrapeJob {

  private static final int CHUNK_SIZE = 200;



  private static final String OUTPUT_FOLDER = "output";



  private static final Logger log = LoggerFactory.getLogger( ScrapeJob.class );

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

  private Queue<String> deleteQueue;

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
    this.deleteQueue = new ConcurrentLinkedDeque<>();
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


    List<CompletableFuture<Void>> futures = buildIdStrs( arr ).stream()
        .map( idListStr -> "OBJECTID%20in%20(" + idListStr + ")" )
        .map( queryStr -> queryUrlStr + "?f=json&outFields=*&where=" + queryStr )
        .map((query) -> Q.submitRequest(query)
            .thenApply( this::writeJSON )
            .thenAccept( collector::addJsonToPool )
            .thenRun( done::incrementAndGet )
            .whenComplete((_null, ex) -> {
              if(ex != null) {
                log.error("Killing job: " + layerUrl);
                MAN.killJob(layerUrl);
              }
            })
        )
        .collect(Collectors.toList());

    CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).join();

    zipFile = collector.zipUpPool();
    isDone = true;
    log.info("Zipped '" + outputZip + "'");
    runAsync(this::deleteJsonFiles);
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

  private String getLayerName() {

    String jsonDetailsUrl = layerUrl + "?f=json";

    return Q.submitSyncRequest(jsonDetailsUrl)
        .orElseThrow( RuntimeException::new )
        .getString( "name" );
  }

  private void deleteJsonFiles() {
    int size = deleteQueue.size();
    while(!deleteQueue.isEmpty()) {
      String fileToDelete = deleteQueue.poll();
      try {
        Files.delete(Paths.get(fileToDelete));
      } catch (IOException e) {
        log.error("Couldn't delete '" + fileToDelete + "'", e);
      }
    }
    log.info("Deleted " + size + " files");
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

  private String writeJSON(JSONObject obj) {
    // e.g. 'output/wetlands1.json'
    String outFile = outputFileBase + current.incrementAndGet() + ".json";
    log.info(outFile);
    try ( BufferedWriter br = new BufferedWriter( new FileWriter( new File( outFile ) ) ) ) {
      obj.write(br);
    } catch ( IOException e ) {
      log.error("Couldn't write '" + outFile + "'", e);
      failJob( "Failed to write a response.. our fault, try again?" );
    }
    return outFile;
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
