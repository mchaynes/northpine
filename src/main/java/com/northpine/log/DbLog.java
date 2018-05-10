package com.northpine.log;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

public class DbLog {

  private static DbLog me;
  private static final Logger log = LoggerFactory.getLogger(DbLog.class);

  private Connection connection;


  private DbLog() throws SQLException {
    String dbUrl = System.getenv("JDBC_DATABASE_URL") +"&ssl=true&sslfactory=org.postgresql.ssl.NonValidatingFactory";
    connection = DriverManager.getConnection(dbUrl);
  }



  public void startRequest(String ip, String url) {
    try {
      var statement = connection.prepareStatement("INSERT INTO Jobs(ip, url, TimeSubmitted) VALUES (?, ?, ?)");
      statement.setString(1, ip);
      statement.setString(2, url);
      statement.setTimestamp(3, Timestamp.from(Instant.now()));
    } catch (SQLException e) {
      log.error("Failed to insert", e);
    }

  }
  public void endRequest(String ip, String url) {
    try {
      var statement = connection.prepareStatement("UPDATE Jobs SET TimeFinished=? WHERE ip=? AND url=?");
      statement.setTimestamp(1, Timestamp.from(Instant.now()));
      statement.setString(2, ip);
      statement.setString(3, url);
      statement.executeUpdate();
    } catch (SQLException e) {
      log.error("Failed to update", e);
    }
  }

  private String getId(String ip, String url) {
    return ip + url;
  }

  public static DbLog get() {
    if(me == null) {
      try {
        me = new DbLog();
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    }
    return me;
  }
}
