package com.github.nexmark.kafka.queries;

import org.apache.commons.cli.*;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpExchange;

public class RunQuery {
    private static final Option APP_NAME = new Option("n", "name", true, "app name");
    private static final Option SERDE = new Option("s", "serde", true, "serde");
    private static final Option CONFIG_FILE = new Option("c", "conf", true, "config file");
    private static final Option DURATION = new Option("d", "duration", true, "duration in seconds");
    private static final Option WARMUP_TIME = new Option("w", "warmup_time", true, "warmup time in seconds");
    private static final Option NUM_SRC_EVENTS = new Option("e", "srcEvents",
            true, "number of src events");

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
        if (envValue != null && !envValue.isEmpty()) {
            return envValue;
        }
        return defaultVal;
    }

    public static void main(final String[] args) throws ParseException, IOException {
        if (args == null || args.length == 0) {
            System.err.println("Usage: --name <q1> --serde json --config <config_file>");
            System.exit(1);
        }
        Options options = getOptions();
        DefaultParser parser = new DefaultParser();
        CommandLine line = parser.parse(options, args, true);
        String appName = line.getOptionValue(APP_NAME.getOpt());
        String serde = line.getOptionValue(SERDE.getOpt());
        String configFile = line.getOptionValue(CONFIG_FILE.getOpt());
        String durStr = line.getOptionValue(DURATION.getOpt());
        String warmupTime = line.getOptionValue(WARMUP_TIME.getOpt());
        int durationMs = durStr == null ? 0 : Integer.parseInt(durStr) * 1000;
        int warmupDuration = warmupTime == null ? 0 : Integer.parseInt(warmupTime) * 1000;

        String srcEventStr = line.getOptionValue(NUM_SRC_EVENTS.getOpt());
        int srcEvents = srcEventStr == null ? 0 : Integer.parseInt(srcEventStr);
        System.out.println("appName: " + appName + " serde: " + serde +
                " configFile: " + configFile + " duration(ms): "
                + durationMs + "warmup(ms): " + warmupDuration +
                " numSrcEvents: " + srcEvents);

        final String bootstrapServers = getEnvValue("BOOTSTRAP_SERVER_CONFIG", "localhost:29092");

        NexmarkQuery query = getNexmarkQuery(appName);
        if (query == null) {
            System.exit(1);
        }

        StreamsBuilder builder;
        try {
            builder = query.getStreamBuilder(bootstrapServers, serde, configFile);
        } catch (IOException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
            return;
        }

        Properties props = query.getProperties(bootstrapServers, durationMs);
        Topology tp = builder.build();
        System.out.println(tp.describe());

        HttpServer server = HttpServer.create(new InetSocketAddress(8090), 0);
        server.createContext("/run", new HttpHandler() {

            @Override
            public void handle(HttpExchange exchange) throws IOException {
                // TODO Auto-generated method stub
                if (warmupDuration != 0) {
                    final KafkaStreams warmStreams = new KafkaStreams(tp, props);
                    CountDownLatch warmupLatch = new CountDownLatch(1);
                    // attach shutdown handler to catch control-c
                    Runtime.getRuntime().addShutdownHook(new Thread("warm-streams-shutdown-hook") {
                        @Override
                        public void run() {
                            warmStreams.close();
                            warmupLatch.countDown();
                        }
                    });
                    Thread warmupT = new Thread(() -> {
                        long timeStart = System.currentTimeMillis();
                        if (warmupDuration != 0 && srcEvents == 0) {
                            try {
                                Thread.sleep(warmupDuration);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                        warmStreams.close();
                        warmupLatch.countDown();
                        long timeEnd = System.currentTimeMillis();
                        double durationSec = ((timeEnd - timeStart) / 1000.0);
                        System.out.println("Warmup takes: " + durationSec + " s");
                    });
                    warmupT.start();
                    try {
                        warmStreams.start();
                        warmupLatch.await();
                    } catch (Throwable e) {
                        System.exit(1);
                    }
                }

                final CountDownLatch latch = new CountDownLatch(1);
                final KafkaStreams streams = new KafkaStreams(tp, props);
                Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
                    @Override
                    public void run() {
                        streams.close();
                        latch.countDown();
                    }
                });
                Thread t = new Thread(() -> {
                    long timeStart = System.currentTimeMillis();
                    if (durationMs != 0 && srcEvents == 0) {
                        while ((System.currentTimeMillis() - timeStart) < durationMs) {
                            try {
                                Thread.sleep(1000);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                            Map<MetricName, ? extends Metric> metric = streams.metrics();
                            metric.forEach((k, v) -> {
                                if (k.group().equals("stream-task-metrics")) {
                                    System.out.println(k.group() + " " + k.name() + ", tags: " + k.tags()
                                            + ", val: " + v.metricValue());
                                }
                            });
                        }
                    } else {
                        boolean done = false;
                        while (!done) {
                            long cur = System.currentTimeMillis();
                            if (cur - timeStart >= 10000) {
                                long currentCount = query.getInputCount();
                                if (currentCount == srcEvents) {
                                    done = true;
                                }
                            }
                            try {
                                Thread.sleep(5); // ms
                            } catch (InterruptedException e) {
                                // TODO Auto-generated catch block
                                e.printStackTrace();
                            }
                        }
                    }

                    streams.close();
                    long timeEnd = System.currentTimeMillis();
                    System.out.println();
                    System.out.println();
                    Map<MetricName, ? extends Metric> metric = streams.metrics();
                    metric.forEach((k, v) -> {
                        System.out.println(k.group() + " " + k.name() + ", tags: " + k.tags()
                                + ", val: " + v.metricValue());
                    });
                    double durationSec = ((timeEnd - timeStart) / 1000.0);
                    System.out.println("Duration: " + durationSec);
                    latch.countDown();
                });

                t.start();
                try {
                    streams.start();
                    latch.await();
                } catch (Throwable e) {
                    System.exit(1);
                }
            }
        });
        server.setExecutor(null);
        server.start();

    }

    private static Options getOptions() {
        Options options = new Options();
        options.addOption(APP_NAME);
        options.addOption(SERDE);
        options.addOption(CONFIG_FILE);
        options.addOption(DURATION);
        options.addOption(NUM_SRC_EVENTS);
        options.addOption(WARMUP_TIME);
        return options;
    }
}
