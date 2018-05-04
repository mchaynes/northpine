package com.northpine.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static spark.Spark.*;

public class Server {

  private static Logger log = LoggerFactory.getLogger( Server.class );

  public static void main(String[] args) {
    checkOgrInstall();
    ServerHandler handler = new ServerHandler();
    Path output = Paths.get("output/");
    if( Files.notExists( output ) ) {
      try {
        Files.createDirectory( output );
        log.info("successfully created '" + output + "'");
      } catch ( IOException e ) {
        log.error( "Couldn't create " + output, e );
        System.exit( 1 );
      }
    }
    port(getHerokuAssignedPort());
    staticFiles.location( "public" );

    before( handler::checkUrlParam );
    post("/scrape",  handler::handleScrapeStartRequest);
    get("/status", handler::handleGetProgress);
    get("/output", handler::handleGetOutput);
    get("/nourl", (req, res) -> "need a url");
    get("layers", handler::handleGetAllLayers);

  }

  private static int getHerokuAssignedPort() {
    ProcessBuilder processBuilder = new ProcessBuilder();
    if (processBuilder.environment().get("PORT") != null) {
      return Integer.parseInt(processBuilder.environment().get("PORT"));
    }
    return 8000; //return default port if PORT isn't set (i.e. on localhost)
  }

  private static void checkOgrInstall() {
    try {
      Runtime rt = Runtime.getRuntime();
      String[] commands = {"ogr2ogr"};
      rt.exec( commands );
    } catch (IOException io) {
      log.info("Something is wrong with ogr2ogr", io);
      System.exit(1);
    }
  }



}
