package com.northpine.scrape.ogr;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Scanner;
import java.util.concurrent.CompletableFuture;

public abstract class OgrCollector {

  protected static final Logger log = LoggerFactory.getLogger(OgrCollector.class);

  protected String poolBase;

  private String ogrFormat;

  private String extension;

  protected OgrCollector(String poolBase, String ogrFormat, String extension) {
    this.poolBase = poolBase;
    this.ogrFormat = ogrFormat;
    this.extension = extension;
  }

  public synchronized void addJsonToPool(String file) {
    ProcessBuilder builder = new ProcessBuilder( "ogr2ogr", "-f", ""+ ogrFormat + "", "-append", poolBase + extension, file );
    try {
      Process p;
      p = builder.start();
      p.waitFor();
      Scanner c = new Scanner(p.getErrorStream());
      while(c.hasNext()) {
        log.error(c.nextLine());
      }
      CompletableFuture.runAsync( () -> {
        try {
          Files.delete( Paths.get( file ) );
        } catch ( IOException e ) {
          log.warn("Couldn't delete " + file, e);
        }
      } );
    } catch ( IOException | InterruptedException e ) {
      log.error("ogr2ogr failed", e);
    }
  }

  public abstract File zipUpPool();
}
