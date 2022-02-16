# NorthPine

## Warning: This repo is deprecated, use [geodatadownloader](https://github.com/mchaynes/geodatadownloader) instead



### Description
This is the webserver code for a service that will turn an ArcGIS Rest Map service into a shapefile. It is not limited to rate limits (i.e. layers with < 1000 features)

### Dependencies
1. Install [Jdk 1.8](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html) 
2. Install [Maven](https://maven.apache.org/download.cgi)
3. Install and setup GDAL [Windows](http://sandbox.idre.ucla.edu/sandbox/tutorials/installing-gdal-for-windows) - [Mac/Linux](https://www.mapbox.com/tilemill/docs/guides/gdal/)

### Getting it to run
1. Make sure you can run java & maven from command line, check with: 
````java -version && mvn --version````
2. run `mvn install`
3. run `java -jar target/NorthPine-1.0-SNAPSHOT-jar-with-dependencies.jar`
