package com.northpine.scrape;

import java.util.List;

public class ArcLayer {

  private final String url;

  private final String name;

  private List<String> attributes;

  public ArcLayer(String url, String name) {
    this.url = url;
    this.name = name;
  }

  public String getUrl() {
    return url;
  }

  public String getName() {
    return name;
  }
}
