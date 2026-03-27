package chatflow.db.db;

import javax.sql.DataSource;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class ConnectionPool {
  private static HikariDataSource dataSource;

  public static void init(String dbHost) {
    HikariConfig config = new HikariConfig();
    config.setJdbcUrl("jdbc:postgresql://" + dbHost + ":5432/chatflow");
    config.setUsername("chatflow");
    config.setPassword("chatflow123");
    config.setDriverClassName("org.postgresql.Driver");

    config.setMaximumPoolSize(20);
    config.setMinimumIdle(5);
    config.setConnectionTimeout(30_000);
    config.setIdleTimeout(600_000);
    config.setMaxLifetime(1_800_000);

    config.addDataSourceProperty("cachePrepStmts", "true");
    config.addDataSourceProperty("prepStmtCacheSize", "250");
    config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
    config.addDataSourceProperty("useServerPrepStmts", "true");

    config.setPoolName("chatflow-db-pool");
    dataSource = new HikariDataSource(config);
    System.out.println("Connection pool initialized: max=" + config.getMaximumPoolSize());
  }

  public static DataSource get() {
    return dataSource;
  }

  public static int getActiveConnections() {
    return dataSource != null ? dataSource.getHikariPoolMXBean().getActiveConnections() : 0;
  }

  public static int getIdleConnections() {
    return dataSource != null ? dataSource.getHikariPoolMXBean().getIdleConnections() : 0;
  }

  public static int getTotalConnections() {
    return dataSource != null ? dataSource.getHikariPoolMXBean().getTotalConnections() : 0;
  }

  public static void close() {
    if (dataSource != null) {
      dataSource.close();
    }
  }
}
