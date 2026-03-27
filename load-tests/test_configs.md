# Load Test Configurations

## Common launch commands

```bash
# Infrastructure
docker compose up -d

# Producer  (port 8080)
java -jar producer/build/libs/producer-all.jar localhost

# Consumer  (port 9090)
java -jar consumer/build/libs/consumer-all.jar localhost 64 128 10

# DB-Server (port 8081)
java -jar db-server/build/libs/db-server-all.jar localhost localhost <batchSize> <flushMs>
```

## Test 1 — Baseline (500 000 messages)
```bash
java -jar client/build/libs/client-all.jar localhost localhost 128 32 localhost
# MessagePool.generateMessage(500_000) is hardcoded in ClientApp
```

## Test 2 — Stress (1 000 000 messages)
```bash
# Edit ClientApp.java: MessagePool.generateMessage(1_000_000)
java -jar client/build/libs/client-all.jar localhost localhost 128 32 localhost
```

## Test 3 — Endurance (~30 min sustained)
```bash
# Edit ClientApp.java: MessagePool.generateMessage(5_000_000)
java -jar client/build/libs/client-all.jar localhost localhost 100 26 localhost
```

