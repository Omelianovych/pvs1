package com.labwork;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.IAtomicLong;
import com.hazelcast.map.IMap;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

public class HazelcastCounter {

    private final HazelcastInstance hz;
    private final String key;
    private final String mapName = "counter-map";
    private final int threadCount;
    private final int incrementsPerThread;
    private final Logger logger;

    public HazelcastCounter(HazelcastInstance hz, String key, int threadCount, int incrementsPerThread, Logger logger) {
        this.hz = hz;
        this.key = key;
        this.threadCount = threadCount;
        this.incrementsPerThread = incrementsPerThread;
        this.logger = logger;
    }

    public void clearData() {
        IMap<String, Value> map = hz.getMap(mapName);
        map.put(key, new Value());
    }

    public void clearAtomicLongData() {
        IAtomicLong atomicLong = hz.getCPSubsystem().getAtomicLong(key);
        atomicLong.set(0);
    }

    public void runNoLockCounter() throws InterruptedException {
        IMap<String, Value> map = hz.getMap(mapName);

        logger.info("Starting increments without locking...");
        long startTime = System.currentTimeMillis();

        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < threadCount; i++) {
            Thread t = new Thread(() -> {
                for (int k = 0; k < incrementsPerThread; k++) {
                    Value value = map.get(key);
                    value.amount++;
                    map.put(key, value);
                }
            });
            threads.add(t);
            t.start();
        }

        for (Thread t : threads) {
            t.join();
        }

        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        int finalValue = map.get(key).amount;
        int expectedValue = threadCount * incrementsPerThread;

        logger.info("Finished (No Lock)");
        logger.info("Expected final value: " + expectedValue);
        logger.info("Actual final value:   " + finalValue);
        logger.info("Time taken: " + duration + " ms");
    }

    public void runPessimisticLockCounter() throws InterruptedException {
        IMap<String, Value> map = hz.getMap(mapName);

        logger.info("Starting increments with pessimistic locking...");
        long startTime = System.currentTimeMillis();

        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < threadCount; i++) {
            Thread t = new Thread(() -> {
                for (int k = 0; k < incrementsPerThread; k++) {
                    map.lock(key);
                    try {
                        Value value = map.get(key);
                        value.amount++;
                        map.put(key, value);
                    } finally {
                        map.unlock(key);
                    }
                }
            });
            threads.add(t);
            t.start();
        }

        for (Thread t : threads) {
            t.join();
        }

        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        int finalValue = map.get(key).amount;
        int expectedValue = threadCount * incrementsPerThread;

        logger.info("Finished (Pessimistic Lock)");
        logger.info("Expected final value: " + expectedValue);
        logger.info("Actual final value:   " + finalValue);
        logger.info("Time taken: " + duration + " ms");
    }

    public void runOptimisticLockCounter() throws InterruptedException {
        IMap<String, Value> map = hz.getMap(mapName);

        logger.info("Starting increments with optimistic locking...");
        long startTime = System.currentTimeMillis();

        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < threadCount; i++) {
            Thread t = new Thread(() -> {
                for (int k = 0; k < incrementsPerThread; k++) {
                    for (; ; ) {
                        Value oldValue = map.get(key);
                        Value newValue = new Value(oldValue);
                        newValue.amount++;
                        if (map.replace(key, oldValue, newValue)) {
                            break;
                        }
                    }
                }
            });
            threads.add(t);
            t.start();
        }

        for (Thread t : threads) {
            t.join();
        }

        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        int finalValue = map.get(key).amount;
        int expectedValue = threadCount * incrementsPerThread;

        logger.info("Finished (Optimistic Lock)!");
        logger.info("Expected final value: " + expectedValue);
        logger.info("Actual final value:   " + finalValue);
        logger.info("Time taken: " + duration + " ms");
    }

    public void runIAtomicLongCounter() throws InterruptedException {
        IAtomicLong atomicLong = hz.getCPSubsystem().getAtomicLong(key);

        logger.info("Starting increments with IAtomicLong...");
        long startTime = System.currentTimeMillis();

        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < threadCount; i++) {
            Thread t = new Thread(() -> {
                for (int k = 0; k < incrementsPerThread; k++) {
                    atomicLong.incrementAndGet();
                }
            });
            threads.add(t);
            t.start();
        }

        for (Thread t : threads) {
            t.join();
        }

        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        long finalValue = atomicLong.get();
        long expectedValue = (long) threadCount * incrementsPerThread;

        logger.info("Finished (IAtomicLong)!");
        logger.info("Expected final value: " + expectedValue);
        logger.info("Actual final value:   " + finalValue);
        logger.info("Time taken: " + duration + " ms");
    }

    static class Value implements Serializable {
        public int amount;

        public Value() {
        }

        public Value(Value that) {
            this.amount = that.amount;
        }

        @Override
        public boolean equals(Object o) {
            if (o == this) return true;
            if (!(o instanceof Value)) return false;
            Value that = (Value) o;
            return this.amount == that.amount;
        }

        @Override
        public int hashCode() {
            return Integer.hashCode(amount);
        }
    }
}
