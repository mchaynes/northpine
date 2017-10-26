package com.northpine.scrape;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class GeoCollector extends OgrCollector {

  private static final String GEO_PACKAGE = "GPKG";

  private static final String EXTENSION = ".gpkg";

  protected GeoCollector(String poolFile) {
    super(poolFile, GEO_PACKAGE, EXTENSION);
  }

  @Override
  File zipUpPool() {
    File zip = new File(poolBase + ".zip");
    try(ZipOutputStream zOut = new ZipOutputStream(new FileOutputStream(zip))) {
      String packageFile = poolBase + EXTENSION;
      ZipEntry entry = new ZipEntry(packageFile);
      zOut.putNextEntry(entry);
      Path packagePath = Paths.get(packageFile);
      if(Files.exists(packagePath)) {
        Files.copy(packagePath, zOut);
      } else {
        log.warn(String.format("'%s' doesn't exist", packageFile));
      }
      zOut.closeEntry();
    } catch (IOException io) {
      log.info("Couldn't zip", io);
      throw new RuntimeException("couldn't zip file");
    }
    return zip;
  }
}
