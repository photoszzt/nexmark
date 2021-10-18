package com.github.nexmark.kafka.queries;

import java.util.Properties;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class RunQuery {
  private static NexmarkQuery getNexmarkQuery(int queryNumber) {
    switch (queryNumber) {
    case 1:
      return new Query1();
    case 2:
      return new Query2();
    case 3:
      return new Query3();
    case 4:
      return new Query4();
    case 5:
      return new Query5();
    case 6:
      return new Query6();
    case 7:
      return new Query7();
    case 8:
      return new Query8();
    case 9:
      return new WindowedAvg();
    default:
      System.err.println("Wrong query number: " + queryNumber);
      return null;
    }
  }

  public static void main(final String[] args) {
    if (args.length != 1) {
      System.err.println("Need to specify query number");
      System.exit(1);
    }
    System.out.println(args.length);
    for (int i = 0; i < args.length; i++) {
      System.out.println(args[i]);
    }

    public static String getEnvValue(String envKey, String defaultVal) {
        String envValue = System.getenv(envKey);
        if(envValue != null && !envValue.isEmpty()) {
            return envValue;
        }
        return defaultVal;
    }

    public static void main(final String[] args) {
        if (args.length != 1) {
            System.err.println("Need to specify query number");
            System.exit(1);
        }
        final String bootstrapServers = getEnvValue("BOOTSTRAP_SERVER_CONFIG", "localhost:29092");
        for (int i = 0; i < args.length; i++) {
            System.out.println(args[i]);
        }
        
        int queryNumber = Integer.parseInt(args[0]);
        NexmarkQuery query = getNexmarkQuery(queryNumber);
        if (query == null) {
            System.exit(1);
        }
        StreamsBuilder builder = query.getStreamBuilder(bootstrapServers);
        Properties props = query.getProperties(bootstrapServers);
        Topology tp = builder.build();
        System.out.println(tp.describe());
        final KafkaStreams streams = new KafkaStreams(tp, props);

        final CountDownLatch latch = new CountDownLatch(1);
        int duration = 60 * 1000;
        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });
        Thread t = new Thread(() -> {
            long timeStart = System.currentTimeMillis();
            try {
                Thread.sleep(duration);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            streams.close();
            long timeEnd = System.currentTimeMillis();
            double durationSec = ((timeEnd - timeStart) / 1000.0);
            Map<String, CountAction> caMap = query.getCountActionMap();
            CountAction caInput = caMap.get("caInput");
            CountAction caOutput = caMap.get("caOutput");
            System.out.println("reading events: " + caInput.GetProcessedRecords());
            System.out.println("output events: " + caOutput.GetProcessedRecords());
            System.out.println("Duration: " + durationSec);
            System.out.println("reading events throughput: " + ((double)caInput.GetProcessedRecords()) / duration);
            System.out.println("output throughput: " + (double)caOutput.GetProcessedRecords() / duration);
            latch.countDown();
        });
        t.start();

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
    StreamsBuilder builder = query.getStreamBuilder();
    Properties props = query.getProperties();
    Topology tp = builder.build();
    System.out.println(tp.describe());
    final KafkaStreams streams = new KafkaStreams(tp, props);
    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    streams.start();
  }
}
