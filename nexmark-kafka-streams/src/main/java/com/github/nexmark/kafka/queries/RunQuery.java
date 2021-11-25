package com.github.nexmark.kafka.queries;

import org.apache.commons.cli.*;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class RunQuery {
    private static final Option APP_NAME = new Option("n", "name", true, "app name");
    private static final Option SERDE = new Option("s", "serde", true, "serde");

    private static NexmarkQuery getNexmarkQuery(String appName) {
        switch (appName) {
            case "q1":
                return new Query1();
            case "q2":
                return new Query2();
            case "q3":
                return new Query3();
            case "q4":
                return new Query4();
            case "q5":
                return new Query5();
            case "q6":
                return new Query6();
            case "q7":
                return new Query7();
            case "q8":
                return new Query8();
            case "windowedAvg":
                return new WindowedAvg();
            default:
                System.err.println("Unrecognized app name: " + appName);
                return null;
        }
    }

    public static String getEnvValue(String envKey, String defaultVal) {
        String envValue = System.getenv(envKey);
        if(envValue != null && !envValue.isEmpty()) {
            return envValue;
        }
        return defaultVal;
    }

    public static void main(final String[] args) throws ParseException {
        if (args == null || args.length == 0) {
            System.err.println("Usage: --name <q1> --serde json");
            System.exit(1);
        }
        Options options = getOptions();
        DefaultParser parser = new DefaultParser();
        CommandLine line = parser.parse(options, args, true);
        String appName = line.getOptionValue(APP_NAME.getOpt());
        String serde = line.getOptionValue(SERDE.getOpt());

        final String bootstrapServers = getEnvValue("BOOTSTRAP_SERVER_CONFIG", "localhost:29092");
        
        NexmarkQuery query = getNexmarkQuery(appName);
        if (query == null) {
            System.exit(1);
        }
        StreamsBuilder builder = query.getStreamBuilder(bootstrapServers, serde);
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
            System.out.println("reading events throughput: " + ((double)caInput.GetProcessedRecords()) / durationSec);
            System.out.println("output throughput: " + (double)caOutput.GetProcessedRecords() / durationSec);
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

    private static Options getOptions() {
        Options options = new Options();
        options.addOption(APP_NAME);
        options.addOption(SERDE);
        return options;
    }
}
