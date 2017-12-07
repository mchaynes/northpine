package com.northpine.server;

import com.northpine.scrape.ArcServer;
import com.northpine.scrape.ScrapeJob;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static spark.Spark.halt;

@SuppressWarnings( {"unused", "ThrowableNotThrown"} )
public class ServerHandler {

  private static final String NO_JOB_FOUND_MESSAGE = "job not found";
  private static final Logger log = LoggerFactory.getLogger( ServerHandler.class );

  private Map<String, ArcServer> servers = new HashMap<>();

  private Map<String, ScrapeJob> scrapeJobs = new HashMap<>();


  void checkUrlParam(Request req, Response res) {
    String urlStr = req.queryMap( "url" ).value();
    if(urlStr == null) {
      halt(400,"url param not supplied");
    }
    try {
      URL url = new URL( urlStr );
    } catch ( MalformedURLException e ) {
      log.error("invalid url '" + urlStr + "'");
      halt(400, "malformed url");
    }
  }

  String handleScrapeStartRequest(Request req, Response res) {
    String url = req.queryMap( "url" ).value();
    JSONObject message = new JSONObject();
    message.accumulate("ip", req.ip());
    message.accumulate("url", url);
    log.info(message.toString());
    if(scrapeJobs.containsKey(url)) {
      //If by some stroke of luck the job has already started and hasn't been cleared, don't restart the job, just pick
      //where you left off.
      return scrapeJobs.get(url).getName();

    } else {
      //Start a new scrape job
      ScrapeJob scraper = new ScrapeJob( url );
      scrapeJobs.put( url, scraper );
      CompletableFuture.runAsync( scraper::startScraping );
      res.cookie( "job", url );
      return scraper.getName();
    }
  }

  public String handleGetProgress(Request req, Response res) {
    String url = req.queryMap( "url" ).value();
    if ( url == null || !scrapeJobs.containsKey( url ) ) {

      return NO_JOB_FOUND_MESSAGE;
    } else {
      ScrapeJob job = scrapeJobs.get( url );
      JSONObject response = new JSONObject();
      res.type("application/json");
      response.accumulate( "finished", job.isJobDone() );
      response.accumulate( "done", job.getNumDone() );
      response.accumulate( "total", job.getTotal() );
      response.accumulate( "layer", job.getName() );
      response.accumulate( "failed", job.isFailed() );
      if(job.isFailed()) {
        response.accumulate( "errorMessage", job.getFailMessage() );
      }
      return response.toString( 2 );
    }
  }

  public String handleReload(Request req, Response res) {

    return "";
  }

  public String handleGetOutput(Request req, Response res) {
    String url = req.queryMap( "url" ).value();
    if( !scrapeJobs.containsKey( url ) ) {
      res.status( 404 );
      return NO_JOB_FOUND_MESSAGE;
    } else if(scrapeJobs.get(url).isFailed()) {
      res.status(500);
      return "job failed... submit another";
    } else {
      File file = new File(scrapeJobs.get(url).getOutput());
      res.raw().setContentType("application/octet-stream");
      res.raw().setHeader("Content-Disposition","attachment; filename="+file.getName() );
      res.raw().setHeader("Content-Length", Long.toString(file.length()));
      try {

        try( BufferedOutputStream bOut = new BufferedOutputStream(res.raw().getOutputStream());
             BufferedInputStream bufferedInputStream = new BufferedInputStream(new FileInputStream(file)))
        {
          byte[] buffer = new byte[1024];
          int len;
          while ((len = bufferedInputStream.read(buffer)) > 0) {
            bOut.write(buffer,0,len);
          }
        }

      } catch (Exception e) {
        halt(405,"server error");
      }
      return "";
    }
  }


  public String handleGetAllLayers(Request req, Response res) {
    try {
      JSONObject jog = new JSONObject();
      String url = req.queryParams("url");
      jog.put("url", url);
      ArcServer server;
      boolean alreadyStarted = servers.containsKey(url);
      jog.put("alreadyStarted", alreadyStarted);
      if(alreadyStarted) {
        server = servers.get(url);
      } else {
        server = new ArcServer(new URI(url));
        servers.put(url, server);
      }
      JSONArray layers = server.getLayers().stream().map(x -> {
        JSONObject obj = new JSONObject();
        obj.put("name", x.getName());
        obj.put("url", x.getUrl());
        return obj;
      }).reduce(new JSONArray(), JSONArray::put, (arr1, arr2) -> {
        for(int i =0;i<arr1.length();i++) arr2.put(arr1.getJSONObject(i));
        return arr2;
      });
      res.type("application/json");
      JSONObject obj = new JSONObject();
      jog.put("size", server.getLayers().size());
      obj.put("layers", layers);
      obj.put("done", server.isDone() && alreadyStarted);
      log.info(jog.toString());
      return obj.toString();
    } catch(Exception io) {
      log.error("didn't work", io);
      return "didn't work :(";
    }
  }



}
