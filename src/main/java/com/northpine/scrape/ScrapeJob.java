package com.northpine.scrape;

import com.northpine.scrape.ogr.GeoCollector;
import com.northpine.scrape.ogr.OgrCollector;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

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
    JSONObject idsJson = getJsonResponse( queryUrl ).orElseThrow( RuntimeException::new );
    JSONArray arr = idsJson.getJSONArray( "objectIds" );
    RequestConfig config = RequestConfig.DEFAULT;
    CloseableHttpAsyncClient httpClient = HttpAsyncClients.custom()
        .setDefaultRequestConfig(config)
        .build();
    httpClient.start();
    OgrCollector collector = new GeoCollector(outputFileBase);
    List<String> idStrs = buildIdStrs( arr );
    CountDownLatch latch = new CountDownLatch( idStrs.size() );
    idStrs.stream()
        .map( idListStr -> "OBJECTID%20in%20(" + idListStr + ")" )
        .map( queryStr -> queryUrlStr + "?f=json&outFields=*&where=" + queryStr )
        .map( HttpGet::new )
        .forEach( request ->
            httpClient.execute(request, new FutureCallback<HttpResponse>() {

              @Override
              public void completed(final HttpResponse response) {
                String body = "";
                try(Scanner scanner = new Scanner(response.getEntity().getContent())) {
                  StringBuilder sb = new StringBuilder();
                  while(scanner.hasNext()) {
                    sb.append( scanner.next() );
                  }
                  body = sb.toString();
                } catch ( IOException io ) {
                  log.error("couldn't get body of response", io);
                  failed.set( true );
                }
                JSONObject jsonObject = new JSONObject( body );
                CompletableFuture.supplyAsync( () -> writeJSON( jsonObject ), executor )
                  .thenAccept( collector::addJsonToPool )
                  .thenRun( latch::countDown )
                  .thenRun(done::incrementAndGet);
              }

              @Override
              public void failed(final Exception ex) {
                latch.countDown();
                log.error(request.getRequestLine() + "->" + ex);
              }

              @Override
              public void cancelled() {
                latch.countDown();
                log.error(request.getRequestLine() + " cancelled");
              }

            })
        );
    try {
      latch.await();
      log.info("Done waiting for responses");
    } catch ( InterruptedException e ) {
      log.error("Couldn't be awaited", e);
    }
    zipFile = collector.zipUpPool();
    isDone = true;
    log.info("Zipped '" + outputZip + "'");
    CompletableFuture.runAsync(this::deleteJsonFiles);
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
    String jsonLayerDeetsUrlStr = layerUrl + "?f=json";
    URL jsonLayerDeetsUrl = getURL( jsonLayerDeetsUrlStr );
    return getJsonResponse( jsonLayerDeetsUrl )
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

  private Optional<JSONObject> getJsonResponse(URL queryUrl) {
    HttpURLConnection connection;
    try {
      connection = ( HttpURLConnection ) queryUrl.openConnection();
      connection.setRequestMethod( "GET" );
      connection.connect();
      InputStream inputStream = connection.getInputStream();
      Reader reader = new InputStreamReader( inputStream );
      Scanner scanner = new Scanner( reader );

      StringBuilder sb = new StringBuilder();
      while ( scanner.hasNext() ) {
        sb.append( scanner.next() );
      }
      JSONObject response = new JSONObject( sb.toString() );
      if(response.has("error")) {
        failJob("Json response contains error");
      }
      return Optional.of(response);
    } catch ( IOException e ) {
      log.error("connection error", e);
      failJob( "reading body of response failed" );
      return Optional.empty();
    } catch (JSONException exception) {
      log.error("layer sent invalid json response, probably not a valid layer", exception);
      failJob( "invalid layer response, is this an arcgis server?" );
      return Optional.empty();
    }
  }

  private void addToShp(String jsonFile) {
    try {
      ProcessBuilder builder = new ProcessBuilder( "ogr2ogr", "-f", "ESRI Shapefile", "-append", outputFileBase + ".shp", jsonFile );
      Process p = builder.start();
      p.waitFor();
      deleteQueue.add(jsonFile);
      done.incrementAndGet();
    } catch ( IOException | InterruptedException e ) {
      log.error("ogr2ogr failed", e);
      failJob( "ogr2ogr failed" );
    }
  }

  private String writeJSON(JSONObject obj) {
    // e.g. 'output/wetlands1.json'
    String outFile = outputFileBase + current.incrementAndGet() + ".json";
    try ( BufferedWriter br = new BufferedWriter( new FileWriter( new File( outFile ) ) ) ) {
      br.write( obj.toString() );
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
