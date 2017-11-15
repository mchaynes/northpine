package com.northpine.scrape;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;

public enum HttpLatch {

  LATCH;

  ConcurrentMap<String, CountDownLatch> latchMap;


  HttpLatch() {
    latchMap = new ConcurrentHashMap<>();
  }

  public void increment(String key) {
    CountDownLatch latch = latchMap.getOrDefault(key, new CountDownLatch(1));

  }

}
