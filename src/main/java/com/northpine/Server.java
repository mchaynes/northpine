package com.northpine;

import static spark.Spark.*;

public class Server {


    public static void main(String[] args) {
      ServerHandler handler = new ServerHandler();

      port(8000);
      staticFiles.location( "public" );
      before( handler::checkUrlParam );
      get("/scrape",  handler::handleScrapeStartRequest);
      get("/status", handler::handleGetProgress);
      get("/output", handler::handleGetOutput);
      get("/nourl", (req, res) -> "need a url");
    }
}
