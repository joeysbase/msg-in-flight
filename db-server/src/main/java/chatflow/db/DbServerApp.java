package chatflow.db;

import chatflow.db.consumer.DbConsumerManager;
import chatflow.db.db.ConnectionPool;
import chatflow.db.db.DbMetricsRepository;
import chatflow.db.db.MessageRepository;
import chatflow.db.db.SchemaInitializer;
import chatflow.db.metrics.AnalyticsService;
import chatflow.db.metrics.MetricsServlet;
import chatflow.db.writer.BatchMessageWriter;
import chatflow.db.writer.CircuitBreaker;
import chatflow.db.writer.DeadLetterQueue;
import chatflow.db.writer.WriterStats;
import org.apache.catalina.Context;
import org.apache.catalina.startup.Tomcat;

import java.io.File;

/**
 * DB Server entry point.
 *
 * Usage:
 *   java DbServerApp <mqHost> <dbHost> <batchSize> <flushIntervalMs>
 *
 * Example:
 *   java DbServerApp localhost localhost 1000 500
 *
 * Defaults if not provided:
 *   batchSize=1000, flushIntervalMs=500
 *
 * HTTP API:
 *   GET http://<host>:8081/metrics  — full metrics JSON
 *   GET http://<host>:8081/health   — liveness probe
 *
 * Architecture:
 *   RabbitMQ (db.room.*) → DbConsumerManager → BatchMessageWriter → PostgreSQL
 *                                                        ↓ (failures)
 *                                              DeadLetterQueue (retry with backoff)
 *
 * Circuit breaker configuration:
 *   threshold=5 failures, recovery=30s
 *
 * Connection pool: HikariCP, max=20 connections
 */
public class DbServerApp {

    private static final int HTTP_PORT          = 8081;
    private static final int CHANNEL_POOL_SIZE  = 30;
    private static final int CONSUMER_POOL_SIZE = 10;
    private static final int WRITER_POOL_SIZE   = 4;
    private static final int PREFETCH_COUNT     = 50;

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println(
                    "Usage: java DbServerApp <mqHost> <dbHost> [batchSize] [flushIntervalMs]");
            System.exit(1);
        }

        String mqHost         = args[0];
        String dbHost         = args[1];
        int    batchSize      = args.length > 2 ? Integer.parseInt(args[2]) : 1000;
        long   flushIntervalMs = args.length > 3 ? Long.parseLong(args[3])  : 500;

        System.out.printf("DbServer starting: mqHost=%s dbHost=%s batchSize=%d flushInterval=%dms%n",
                mqHost, dbHost, batchSize, flushIntervalMs);

        // 1. Database connection pool + schema
        ConnectionPool.init(dbHost);
        SchemaInitializer.init(ConnectionPool.get());

        // 2. Repositories
        MessageRepository   repo      = new MessageRepository(ConnectionPool.get());
        DbMetricsRepository dbMetrics = new DbMetricsRepository(ConnectionPool.get());

        // 3. Write-behind infrastructure
        WriterStats stats = new WriterStats();
        CircuitBreaker cb = new CircuitBreaker(5, 30_000);

        // DLQ retries by re-submitting to writer (writer is set after construction)
        DeadLetterQueue dlq = new DeadLetterQueue(
                messages -> {}, // placeholder, replaced below after writer is created
                stats);

        BatchMessageWriter writer = new BatchMessageWriter(
                repo, cb, dlq, stats, batchSize, flushIntervalMs, WRITER_POOL_SIZE);

        // Wire DLQ retry callback to writer
        DeadLetterQueue dlqFinal = new DeadLetterQueue(
                messages -> writer.submitAll(messages), stats);

        // 4. RabbitMQ consumer
        DbConsumerManager consumerManager = new DbConsumerManager(
                mqHost, CHANNEL_POOL_SIZE, CONSUMER_POOL_SIZE, PREFETCH_COUNT, writer);

        // 5. Analytics
        AnalyticsService analytics = new AnalyticsService(repo, dbMetrics, stats, cb, dlqFinal, writer);
        MetricsServlet metricsServlet = new MetricsServlet(analytics);

        // 6. Start components
        writer.start();
        Thread dlqThread = new Thread(dlqFinal, "dlq-retry");
        dlqThread.setDaemon(true);
        dlqThread.start();

        Thread consumerThread = new Thread(consumerManager, "db-consumer-manager");
        consumerThread.setDaemon(true);
        consumerThread.start();

        // 7. Shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("DbServer shutting down...");
            consumerManager.stop();
            writer.stop();
            dlqFinal.stop();
            ConnectionPool.close();
            System.out.println("DbServer stopped.");
        }));

        // 8. HTTP server for metrics API (port 8081, no WebSocket needed)
        Tomcat tomcat = new Tomcat();
        tomcat.setPort(HTTP_PORT);
        tomcat.getConnector();
        tomcat.setBaseDir(new File("target/tomcat-db").getAbsolutePath());

        Context context = tomcat.addContext("", new File(".").getAbsolutePath());
        Tomcat.addServlet(context, "default", new org.apache.catalina.servlets.DefaultServlet());
        context.addServletMappingDecoded("/", "default");
        Tomcat.addServlet(context, "metricsServlet", metricsServlet);
        context.addServletMappingDecoded("/metrics", "metricsServlet");
        context.addServletMappingDecoded("/health", "metricsServlet");

        tomcat.start();
        System.out.println("DbServer started. Metrics API: http://0.0.0.0:" + HTTP_PORT + "/metrics");
        tomcat.getServer().await();
    }
}
