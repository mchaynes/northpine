package com.northpine.scrape;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public enum JobManager {

  M;


  private Map<String, ScrapeJob> jobs;
  private Map<String, Integer> numRequesters;


  JobManager() {
    jobs = new ConcurrentHashMap<>();
    numRequesters = new ConcurrentHashMap<>();
  }

  public String submitJob(String url) {
    if(jobs.containsKey(url)) {
      numRequesters.computeIfPresent(url, (key, num) -> num + 1);
      numRequesters.putIfAbsent(url, 1);
      return jobs.get(url).getName();
    }
    else {
      ScrapeJob job = new ScrapeJob(url);
      jobs.put(url, job);
      numRequesters.put(url, 1);
      job.startScraping();
      return job.getName();
    }
  }


  public void killJob(String url) {
    if(numRequesters.containsKey(url) && numRequesters.get(url) == 1) {
      jobs.get(url).stopJob();
      numRequesters.remove(url);
    }
  }

  public Optional<ScrapeJob> getJob(String url) {
      return Optional.of(jobs.get(url));
  }


}
