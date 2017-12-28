package com.northpine.scrape.ogr;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class ShapeCollector extends OgrCollector {

  private static final String OGR_SHAPEFILE = "ESRI Shapefile";

  private static final List<String> SHP_FILE_EXTENSIONS = Arrays.asList(".shp", ".prj", ".shx", ".dbf");
  private static final String WEB_MERCATOR_PRJ = "PROJCS[\"WGS_1984_Web_Mercator_Auxiliary_Sphere\",GEOGCS[\"GCS_WGS_1984\",DATUM[\"D_WGS_1984\",SPHEROID[\"WGS_1984\",6378137.0,298.257223563]],PRIMEM[\"Greenwich\",0.0],UNIT[\"Degree\",0.0174532925199433]],PROJECTION[\"Mercator_Auxiliary_Sphere\"],PARAMETER[\"False_Easting\",0.0],PARAMETER[\"False_Northing\",0.0],PARAMETER[\"Central_Meridian\",0.0],PARAMETER[\"Standard_Parallel_1\",0.0],PARAMETER[\"Auxiliary_Sphere_Type\",0.0],UNIT[\"Meter\",1.0]]";

  private static final String EXTENSION = ".shp";


  public ShapeCollector(String poolBase) {
    super(poolBase, OGR_SHAPEFILE, EXTENSION);
  }

  @Override
  public File zipUpPool() {
    File zip  = new File(poolBase + ".zip");
    try (ZipOutputStream zOut = new ZipOutputStream( new FileOutputStream( zip ) )) {
      SHP_FILE_EXTENSIONS.forEach( ext -> {
        try {
          Path pathToShp = Paths.get(poolBase + ext);
          ZipEntry entry = new ZipEntry( poolBase + ext );
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
//            failJob( "ogr2ogr2 failed somewhere" );
          }
          zOut.closeEntry();
        } catch ( IOException e ) {
          log.error("Couldn't zip '" + poolBase + ext + "'", e);
//          failJob("Couldn't zip up file");
        }
      } );
    } catch ( IOException e ) {
      log.error("Couldn't open zip '" + poolBase  + ".zip'", e);
    }
    return zip;
  }
}
