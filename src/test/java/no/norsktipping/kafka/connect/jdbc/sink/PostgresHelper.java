package no.norsktipping.kafka.connect.jdbc.sink;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.HashSet;
import java.util.Properties;


public final class PostgresHelper {

  static {
    try {
      Class.forName("org.postgresql.Driver");
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  public interface ResultSetReadCallback {
    void read(final ResultSet rs) throws SQLException;
  }

  public final Path dbPath;

  public Connection connection;

  public PostgresHelper(String testId) {
    dbPath = Paths.get(testId);
  }

  public String postgreSQL() {
    return "jdbc:postgresql:" + dbPath;
  }
  public static final HashSet<String> tablesUsed = new HashSet<>();

  public void setUp() throws SQLException, IOException {
    Files.deleteIfExists(dbPath);
    Properties props = new Properties();
    props.setProperty("user","postgres");
    props.setProperty("password","password123");
    connection = DriverManager.getConnection((postgreSQL()), props);
    connection.setAutoCommit(false);
  }

  public void tearDown() throws SQLException, IOException {
    Files.deleteIfExists(dbPath);
    tablesUsed.forEach(table -> {
      try {
        deleteTable(table);
      } catch (SQLException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    });
    connection.close();
  }

  public void createTable(final String createSql) throws SQLException {
    execute(createSql);
  }

  public void deleteTable(final String table) throws SQLException {
    execute("DROP TABLE IF EXISTS \"" + table +"\"");

    //random errors of table not being available happens in the unit tests
    try {
      Thread.sleep(100);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public int select(final String query, final PostgresHelper.ResultSetReadCallback callback) throws SQLException {
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
      connection.commit();
    }
  }

}