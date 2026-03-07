package chatflow.consumer;

import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import jakarta.websocket.Session;

public class IdempotencyCache {
    private static final int MAX_CACHE_SIZE = 10000;
    
    private static final Map<String, Set<Session>> cache=new ConcurrentHashMap<>(MAX_CACHE_SIZE);
    public static final Map<String, Instant> createdTime=new ConcurrentHashMap<>(MAX_CACHE_SIZE);

    public static void add(String idempotencyKey, Session session){
        cache.computeIfAbsent(idempotencyKey, k->ConcurrentHashMap.newKeySet());
        createdTime.put(idempotencyKey, Instant.now());
        cache.get(idempotencyKey).add(session);
    }

    public static void remove(String idempotencyKey){
        cache.remove(idempotencyKey);
        createdTime.remove(idempotencyKey);
    }

    public static boolean isMessageSent(String idempotencyKey, Session session){
        if(!cache.containsKey(idempotencyKey)){
            return false;
        }
        return cache.get(idempotencyKey).contains(session);
    }
}
