FROM fabric8/java-alpine-openjdk8-jdk:latest
RUN echo 'http://dl-3.alpinelinux.org/alpine/edge/testing' >> /etc/apk/repositories && \
  echo 'http://dl-3.alpinelinux.org/alpine/edge/community' >> /etc/apk/repositories && \
  apk update && \
  apk upgrade && \
  apk add gdal maven

RUN mkdir /app/
ADD . /app/
RUN cd /app/ && \
  mvn clean install
EXPOSE 8000
CMD java -jar  /app/target/NorthPine-1.0-SNAPSHOT-jar-with-dependencies.jar
