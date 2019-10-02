/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.jdbc.sink;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public final class NetezzaHelper {

  static {
    try {
      Class.forName("org.netezza.Driver");
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  public interface ResultSetReadCallback {
    void read(final ResultSet rs) throws SQLException;
  }

  public final Path dbPath;

  public Connection connection;

  public NetezzaHelper(String testId) {
    dbPath = Paths.get(testId);
  }

  public String postgreSQL() {
    return "jdbc:netezza://REPLACE/" + dbPath;
  }

  public void setUp() throws SQLException, IOException {
    Files.deleteIfExists(dbPath);
    Properties props = new Properties();
    props.setProperty("user","REPLACE");
    props.setProperty("password","REPLACE");
    props.setProperty("schema", "");

    connection = DriverManager.getConnection((postgreSQL()), props);
    connection.setAutoCommit(true);
  }

  public void tearDown() throws SQLException, IOException {
    connection.close();
    Files.deleteIfExists(dbPath);
  }

  public void createTable(final String createSql) throws SQLException {
    execute(createSql);
  }

  public void deleteTable(final String table) throws SQLException {
    execute("DROP TABLE " + table + " IF EXISTS" );

    //random errors of table not being available happens in the unit tests
    try {
      Thread.sleep(100);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public int select(final String query, final NetezzaHelper.ResultSetReadCallback callback) throws SQLException {
    int count = 0;
    try (Statement stmt = connection.createStatement()) {
      try (ResultSet rs = stmt.executeQuery(query)) {
        while (rs.next()) {
          callback.read(rs);
          count++;
        }
      }
    }
    return count;
  }

  public void execute(String sql) throws SQLException {
    try (Statement stmt = connection.createStatement()) {
      stmt.executeUpdate(sql);
      //connection.commit();
    }
  }

}