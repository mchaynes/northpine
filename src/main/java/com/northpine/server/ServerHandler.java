package com.northpine.server;

import com.northpine.scrape.ScrapeJob;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Optional;

import static com.northpine.scrape.JobManager.MAN;
import static spark.Spark.halt;

@SuppressWarnings( {"unused", "ThrowableNotThrown"} )
public class ServerHandler {

  private static final String NO_JOB_FOUND_MESSAGE = "job not found";
  private static final Logger log = LoggerFactory.getLogger( ServerHandler.class );

//  private Map<String, ScrapeJob> scrapeJobs = new HashMap<>();


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
    return MAN.submitJob(url).getName();
  }

  String handleGetProgress(Request req, Response res) {
    String url = req.queryMap( "url" ).value();
    Optional<ScrapeJob> optJob = MAN.getJob(url);
    if ( optJob.isPresent() ) {
      ScrapeJob job = optJob.get();
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
      return response.toString();
    } else {
      return NO_JOB_FOUND_MESSAGE;
    }
  }

  public String handleReload(Request req, Response res) {

    return "";
  }

  String handleGetOutput(Request req, Response res) {
    String url = req.queryMap( "url" ).value();
    Optional<ScrapeJob> optJob = MAN.getJob(url);
    if(optJob.isPresent()) {
      ScrapeJob job = optJob.get();
      if(job.isFailed()) {
        res.status(500);
        return "job failed... submit another";
      } else {
        File file = new File(job.getOutput());
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
    } else {
      res.status( 404 );
      return NO_JOB_FOUND_MESSAGE;
    }

  }


}
