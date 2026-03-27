package chatflow.client;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ClientApp {

    public static void main(String[] args) {
        if (args.length < 4 || args.length > 5) {
                System.err.println("Usage: java ClientApp <send_server_ip> <receive_server_ip> <number_of_threads> <number_of_send_connection> [db_s···erver_ip]");
                System.exit(1);
            }
        Integer NUMTHREADS=Integer.valueOf(args[2]);
        Integer NUMSENDCONN=Integer.valueOf(args[3]);
        String dbServerIp = args.length == 5 ? args[4] : null;
        ThroughputMonitor monitor = new ThroughputMonitor(1);
        ProgressMonitor progressMonitor = new ProgressMonitor(1);
        ExecutorService testPool = Executors.newFixedThreadPool(NUMTHREADS);
        ExecutorService monitorPool = Executors.newFixedThreadPool(5);

        String sendServerIp = args[0];
        String receiveServerIp = args[1];
        ConnectionManager connectionManager = new ConnectionManager(sendServerIp, receiveServerIp, NUMSENDCONN);
        connectionManager.init();
        try {
            long totalTime = 0;
            
            System.out.println("Generating warmup messages...");
            MessagePool.generateMessage(500000);
            System.out.println("Warmup phase...");
            // monitorPool.submit(progressMonitor);
            monitorPool.submit(monitor);
            long warmupStartTime = System.currentTimeMillis();
            for (int i = 0; i < NUMTHREADS; i++){
                testPool.submit(new ClientThread(connectionManager));
            }
            testPool.shutdown();
            testPool.awaitTermination(60, TimeUnit.MINUTES);
            long warmupEndTime = System.currentTimeMillis();
            totalTime += warmupEndTime - warmupStartTime;
            monitor.stop();
            // progressMonitor.stop();
            System.out.println("Warmup completed in " + (warmupEndTime - warmupStartTime) / 1000 + " s");
            System.out.println("Total time: " + totalTime / 1000 + " s");
            System.out.println("Throughput: " + (Statistics.succeedMessage.get() / (totalTime / 1000)) + " msg/s");

            // Give the db-server time to flush remaining messages before querying metrics
            if (dbServerIp != null) {
                System.out.println("Waiting 15s for db-server to finish writing...");
                Thread.sleep(15_000);
                fetchAndLogDbMetrics(dbServerIp);
            }

            Thread.sleep(1000*60*10);
        } catch (InterruptedException e) {
            System.err.println("ClientApp interrupted: " + e.getMessage());
            monitor.stop();
            progressMonitor.stop();
            // warmupPool.shutdownNow();
            testPool.shutdownNow();
        }finally {
            connectionManager.cleanup();
            monitor.stop();
            progressMonitor.stop();
            // warmupPool.shutdownNow();
            testPool.shutdownNow();
        }
    }

    private static void fetchAndLogDbMetrics(String dbServerIp) {
        String url = "http://" + dbServerIp + ":8081/metrics";
        System.out.println("========== DB METRICS ==========");
        System.out.println("Fetching: " + url);
        try {
            HttpClient client = HttpClient.newHttpClient();
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .GET()
                    .build();
            HttpResponse<String> response =
                    client.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200) {
                System.out.println(response.body());
            } else {
                System.err.println("Metrics API returned HTTP " + response.statusCode());
            }
        } catch (Exception e) {
            System.err.println("Failed to fetch metrics from " + url + ": " + e.getMessage());
        }
        System.out.println("================================");
    }
    
}
