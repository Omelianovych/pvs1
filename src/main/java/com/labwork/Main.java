package com.labwork;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class Main {
    public static void main(String[] args) throws Exception {
        Logger hazelcastLogger = Logger.getLogger("");
        FileHandler hazelcastFileHandler = new FileHandler("hazelcast.log");
        hazelcastFileHandler.setFormatter(new SimpleFormatter());
        hazelcastLogger.addHandler(hazelcastFileHandler);

        Logger testLogger = Logger.getLogger("TestResults");
        FileHandler testFileHandler = new FileHandler("test-results.log");
        testFileHandler.setFormatter(new SimpleFormatter());
        testLogger.addHandler(testFileHandler);


        Config config = new Config();
        config.setClusterName("my-cluster");
        config.getCPSubsystemConfig().setCPMemberCount(3);

        HazelcastInstance hz1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance hz2 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance hz3 = Hazelcast.newHazelcastInstance(config);



        int threadCount = 10;
        int incrementsPerThread = 10000;

        HazelcastCounter counter = new HazelcastCounter(hz1, "counterKey", threadCount, incrementsPerThread, testLogger);

        counter.clearData();
        counter.runNoLockCounter();

        counter.clearData();
        counter.runPessimisticLockCounter();

        counter.clearData();
        counter.runOptimisticLockCounter();

        counter.clearAtomicLongData();
        counter.runIAtomicLongCounter();

        hz1.shutdown();
        hz2.shutdown();
        hz3.shutdown();
        testLogger.info("Hazelcast Cluster Shutdown.");
    }
}
