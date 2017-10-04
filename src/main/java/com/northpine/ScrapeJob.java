package com.northpine;

import org.apache.http.HttpException;
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
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * Scrapes ArcGIS REST Servers
 */
public class ScrapeJob {

  private static final int CHUNK_SIZE = 200;

  private static final String WEB_MERCATOR_PRJ = "PROJCS[\"WGS_1984_Web_Mercator_Auxiliary_Sphere\",GEOGCS[\"GCS_WGS_1984\",DATUM[\"D_WGS_1984\",SPHEROID[\"WGS_1984\",6378137.0,298.257223563]],PRIMEM[\"Greenwich\",0.0],UNIT[\"Degree\",0.0174532925199433]],PROJECTION[\"Mercator_Auxiliary_Sphere\"],PARAMETER[\"False_Easting\",0.0],PARAMETER[\"False_Northing\",0.0],PARAMETER[\"Central_Meridian\",0.0],PARAMETER[\"Standard_Parallel_1\",0.0],PARAMETER[\"Auxiliary_Sphere_Type\",0.0],UNIT[\"Meter\",1.0]]";

  private static final String OUTPUT_FOLDER = "output";

  private static final List<String> SHP_FILE_EXTENSIONS = Arrays.asList(".shp", ".prj", ".shx", ".dbf");

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
                if(!jsonObject.isNull( "error" )) {
//                  failJob( "json response contains error: " + jsonObject.getString( "error" ), null );
                }
                CompletableFuture.supplyAsync( () -> writeJSON( jsonObject ), executor )
                  .thenAccept( str -> addToShp( str ) )
//                  .exceptionally( (ex) -> failJob( ex.getMessage(), ex ) )
                  .thenRun( latch::countDown );
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
    zipUpShp();
    isDone = true;
    log.info("Zipped '" + outputZip + "'");
  }

  public int getNumDone() {
    if(done != null) {
      return done.get();
    } else {
      return -1;
    }
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

    return getJsonResponse( jsonLayerDeetsUrl ).orElseThrow( RuntimeException::new ).getString( "name" );
  }

  private void zipUpShp() {
    try (ZipOutputStream zOut = new ZipOutputStream( new FileOutputStream( new File( outputZip ) ) )) {
      SHP_FILE_EXTENSIONS.forEach( ext -> {
        try {
          Path pathToShp = Paths.get(outputFileBase + ext);
          ZipEntry entry = new ZipEntry( layerName + ext );
          zOut.putNextEntry( entry );
          if( Files.exists( pathToShp ) ) {
            Files.copy(pathToShp, zOut);
            Files.deleteIfExists( pathToShp );
          }
          else if (".prj".equals( ext )) {
            log.warn("writing default web_mercator prj");
            zOut.write( WEB_MERCATOR_PRJ.getBytes() );
          }
          else {
            failJob( "ogr2ogr2 failed somewhere" );
          }
          zOut.closeEntry();
        } catch ( IOException e ) {
          log.error("Couldn't zip '" + outputFileBase + ext + "'", e);
          failJob("Couldn't zip up file");
        }
      } );
    } catch ( IOException e ) {
      log.error("Couldn't open zip '" + outputZip + "'", e);
    }
  }

  public String getOutput() {
    if ( isJobDone() ) {
      return outputFileBase + ".zip";
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
      return Optional.of(new JSONObject( sb.toString() ));
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
      CompletableFuture.runAsync( () -> {
        try {
          Files.delete( Paths.get( jsonFile ) );
        } catch ( IOException e ) {
          log.warn("Couldn't delete " + jsonFile, e);
        }
      } );
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
