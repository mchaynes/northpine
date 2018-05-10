package com.northpine.scrape;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.concurrent.CompletableFuture.runAsync;

public enum JobManager {

  MAN;


  private Map<String, ScrapeJob> jobs;
  private Map<String, Integer> numRequesters;
  private static final Logger log = LoggerFactory.getLogger(JobManager.class);


  JobManager() {
    jobs = new ConcurrentHashMap<>();
    numRequesters = new ConcurrentHashMap<>();
  }

  public ScrapeJob submitJob(String url) {
    if(jobs.containsKey(url)) {
      numRequesters.computeIfPresent(url, (_key, num) -> num + 1);
      numRequesters.putIfAbsent(url, 1);
      return jobs.get(url);
    }
    else {
      ScrapeJob job = new ScrapeJob(url);
      jobs.put(url, job);
      numRequesters.put(url, 1);
      runAsync(() -> {
        try {
          job.startScraping();
        } catch(IOException io) {
          log.error("Job failed", io);
        }
      });
      return job;
    }
  }


  public void killJob(String url) {
    if(numRequesters.containsKey(url) && numRequesters.get(url) == 1) {
      jobs.get(url).stopJob();
      numRequesters.remove(url);
    }
  }

  public Optional<ScrapeJob> getJob(String url) {
      return Optional.ofNullable(jobs.get(url));
  }

}
