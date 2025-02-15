package com.github.nexmark.kafka.queries;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

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
    private static final Option PORT = new Option("p", "port", true, "port to listen");
    private static final Option FLUSHMS = new Option("f", "flushms", true, "flush interval in ms");
    private static final Option GUARANTEE = new Option("g", "guarantee", true, "guarantee");
    private static final Option DISABLE_CACHE = new Option("xc", "disable_cache", false, "disable cache");
    private static final Option DISABLE_BATCHING = new Option("xb", "disable_batching", false, "disable batching");
    private static final Option STATS_DIR = new Option("sd", "stats_dir", true, "stats dir");

    private static final Option PRODUCER_BATCH_SIZE = new Option("mb", "batch_size", true, "producer max batch size");

    private static final long TEN_SEC_NANO = TimeUnit.NANOSECONDS.convert(10, TimeUnit.SECONDS);

    private static NexmarkQuery getNexmarkQuery(final String appName,
                                                final File statsDir) throws IOException {
        final String canStatsDir = statsDir.getCanonicalPath();
        switch (appName) {
            case "q1":
                return new Query1(statsDir);
            case "q2":
                return new Query2(statsDir);
            case "q3":
                return new Query3(canStatsDir);
            case "q4":
                return new Query4(canStatsDir);
            case "q5":
                return new Query5(canStatsDir);
            case "q6":
                return new Query6(canStatsDir);
            case "q7":
                return new Query7(canStatsDir);
            case "q8":
                return new Query8(canStatsDir);
            case "twoEmpty":
                return new TwoStageEmptyQuery(canStatsDir);
            case "threeEmpty":
                return new ThreeStageEmptyQuery(canStatsDir);
            case "fourEmpty":
                return new FourStageEmptyQuery(canStatsDir);
            default:
                System.err.println("Unrecognized app name: " + appName);
                return null;
        }
    }

    public static String getEnvValue(final String envKey, final String defaultVal) {
        final String envValue = System.getenv(envKey);
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
        final Options options = getOptions();
        final DefaultParser parser = new DefaultParser();
        final CommandLine line = parser.parse(options, args, true);
        final String appName = line.getOptionValue(APP_NAME.getOpt());
        final String serde = line.getOptionValue(SERDE.getOpt());
        final String configFile = line.getOptionValue(CONFIG_FILE.getOpt());
        final String durStr = line.getOptionValue(DURATION.getOpt());
        final String warmupTime = line.getOptionValue(WARMUP_TIME.getOpt());
        final String portStr = line.getOptionValue(PORT.getOpt());
        final String flushStr = line.getOptionValue(FLUSHMS.getOpt());
        final String guarantee = line.getOptionValue(GUARANTEE.getOpt());
        final String statsDirStr = line.getOptionValue(STATS_DIR.getOpt());
        final String prodBatchSizeStr = line.getOptionValue(PRODUCER_BATCH_SIZE.getOpt());

        final int durationMs = durStr == null ? 0 : Integer.parseInt(durStr) * 1000;
        final int warmupDuration = warmupTime == null ? 0 : Integer.parseInt(warmupTime) * 1000;
        final int warmupDurNano = warmupDuration * 1000_000;
        final int port = portStr == null ? 8090 : Integer.parseInt(portStr);
        final int flushms = flushStr == null ? 100 : Integer.parseInt(flushStr);
        final int prodBatchSize = prodBatchSizeStr == null ? 128*1024 : Integer.parseInt(prodBatchSizeStr);
        final boolean disableCache = line.hasOption(DISABLE_CACHE.getOpt());
        final boolean disableBatching = line.hasOption(DISABLE_BATCHING.getOpt());
        final File statsDir = new File(statsDirStr);
        if (!statsDir.exists()) {
            statsDir.mkdirs();
        }

        final String srcEventStr = line.getOptionValue(NUM_SRC_EVENTS.getOpt());
        final int srcEvents = srcEventStr == null ? 0 : Integer.parseInt(srcEventStr);
        System.out.println("appName: " + appName + " serde: " + serde +
            " configFile: " + configFile + " duration(ms): "
            + durationMs + " warmup(ms): " + warmupDuration +
            " numSrcEvents: " + srcEvents + " flushMs: " + flushms +
            " guarantee: " + guarantee + " disableCache: " + disableCache +
            " disableBatching: " + disableBatching + " statsDir: " + statsDirStr);
        if (!guarantee.equals("alo") && !guarantee.equals("eo")) {
            System.out.printf("unrecognized guarantee: %s; expected either alo or eo\n", guarantee);
            return;
        }

        final String bootstrapServers = getEnvValue("BOOTSTRAP_SERVER_CONFIG", "localhost:29092");

        final NexmarkQuery query = getNexmarkQuery(appName, statsDir);
        if (query == null) {
            System.exit(1);
        }

        final StreamsBuilder builder;
        try {
            builder = query.getStreamBuilder(bootstrapServers, serde, configFile);
        } catch (final IOException e1) {
            e1.printStackTrace();
            return;
        }

        final Properties props;
        if (guarantee.equals("alo")) {
            props = query.getAtLeastOnceProperties(bootstrapServers, durationMs, flushms, disableCache,
                disableBatching, prodBatchSize);
        } else {
            props = query.getExactlyOnceProperties(bootstrapServers, durationMs, flushms, disableCache,
                disableBatching, prodBatchSize);
        }
        final Topology tp = builder.build();
        System.out.println(tp.describe());

        System.out.printf("nexmark listening %d\n", port);
        final CountDownLatch latch = new CountDownLatch(1);
        final KafkaStreams streams = new KafkaStreams(tp, props);
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });
        final Thread t = new Thread(() -> {
            final long timeStartNano = System.nanoTime();
            long afterWarmStartNano = 0;
            boolean afterWarmup = false;
            long emitMetricTimer = System.nanoTime();
            final long durationNano = TimeUnit.NANOSECONDS.convert(durationMs, TimeUnit.MILLISECONDS);
            if (durationMs != 0 && srcEvents == 0) {
                while (true) {
                    final long now = System.nanoTime();
                    final long elapsedNano = now - timeStartNano;

                    final long elapsedMetricNano = now - emitMetricTimer;
                    if (elapsedMetricNano >= TEN_SEC_NANO) {
                        System.out.println("emit metrics at " + elapsedNano + " ns");
                        final Map<MetricName, ? extends Metric> metric = streams.metrics();
                        metric.forEach((k, v) -> {
                            final String g = k.group();
                            if (!(g.equals("app-info") || g.startsWith("admin-client") ||
                                g.equals("kafka-metrics-count"))) {
                                try {
                                    if (v.metricValue() instanceof Double) {
                                        final Double d = ((Number) v.metricValue()).doubleValue();
                                        if (!d.isNaN() && d != 0.0) {
                                            System.out.println(k.group() + " " + k.name() + ";tags:" + k.tags()
                                                               + ";val:" + d);
                                        }
                                    }
                                } catch (final ClassCastException e) {
                                    e.printStackTrace();
                                }
                            }
                        });
                        System.out.println();
                        emitMetricTimer = System.nanoTime();
                    }
                    if (elapsedNano >= durationNano) {
                        System.out.println("emit metrics at " + elapsedNano);
                        final Map<MetricName, ? extends Metric> metric = streams.metrics();
                        metric.forEach((k, v) -> {
                            final String g = k.group();
                            if (!(g.equals("app-info") || g.startsWith("admin-client") ||
                                g.equals("kafka-metrics-count"))) {
                                try {
                                    if (v.metricValue() instanceof Double) {
                                        final Double d = ((Number) v.metricValue()).doubleValue();
                                        if (!d.isNaN() && d != 0.0) {
                                            System.out.println(k.group() + " " + k.name() + ";tags:" + k.tags()
                                                               + ";val:" + d);
                                        }
                                    }
                                } catch (final ClassCastException e) {
                                    e.printStackTrace();
                                }
                            }
                        });
                        System.out.println();
                        break;
                    }
                    if (!afterWarmup && elapsedNano >= warmupDurNano) {
                        afterWarmup = true;
                        query.setAfterWarmup();
                        afterWarmStartNano = System.nanoTime();
                    }
                    try {
                        Thread.sleep(10);
                    } catch (final InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
            // else {
            // boolean done = false;
            // while (!done) {
            // long cur = System.nanoTime();
            // if (cur - timeStartNano >= TEN_SEC_NANO) {
            // long currentCount = query.getInputCount();
            // if (currentCount == srcEvents) {
            // done = true;
            // }
            // }
            // try {
            // Thread.sleep(5); // ms
            // } catch (InterruptedException e) {
            // e.printStackTrace();
            // }
            // }
            // }
            final long timeEndNano = System.nanoTime();

            streams.close();
            final long timeEndAfterClose = System.nanoTime();
            query.outputRemainingStats();
            // query.printRemainingStats();
            // System.out.println();
            // System.out.println();
            final Map<MetricName, ? extends Metric> metric = streams.metrics();
            metric.forEach((k, v) -> System.out.println(k.group() + " " + k.name() + ";tags:" + k.tags()
                + ";val:" + v.metricValue()));
            final double durationSec = (timeEndNano - timeStartNano) / 1000_000_000.0;
            final double durationAfterStreamCloseSec = (timeEndAfterClose - timeStartNano) / 1000_000_000.0;
            System.out.println("Duration: " + durationSec + " after streams close: " + durationAfterStreamCloseSec);
            if (warmupDuration != 0) {
                System.out.println("Duration after warmup: " + (timeEndNano - afterWarmStartNano) / 1000_000_000.0);
            }
            System.out.println("Num output: ");
            query.printCount();
            System.out.println();
            latch.countDown();
        });
        streams.start();
        System.err.println("done preparation");
        final HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/run", new HttpHandler() {

            @Override
            public void handle(final HttpExchange exchange) throws IOException {
                System.out.println("Got connection\n");

                t.start();
                System.out.println("Start processing and waiting for result\n");
                try {
                    latch.await();
                } catch (final Throwable e) {
                    System.exit(1);
                }

                final byte[] response = "done nexmark".getBytes();
                exchange.sendResponseHeaders(200, response.length);
                final OutputStream os = exchange.getResponseBody();
                os.write(response);
                os.close();
            }
        });
        server.setExecutor(null);
        server.start();

    }

    //public static long percentile(List<Long> latencies, double percentile) {
    //    int index = (int) Math.ceil(percentile / 100.0 * latencies.size());
    //    return latencies.get(index - 1);
    //}

    private static Options getOptions() {
        final Options options = new Options();
        options.addOption(APP_NAME);
        options.addOption(SERDE);
        options.addOption(CONFIG_FILE);
        options.addOption(DURATION);
        options.addOption(NUM_SRC_EVENTS);
        options.addOption(WARMUP_TIME);
        options.addOption(PORT);
        options.addOption(FLUSHMS);
        options.addOption(GUARANTEE);
        options.addOption(DISABLE_CACHE);
        options.addOption(STATS_DIR);
        options.addOption(DISABLE_BATCHING);
        options.addOption(PRODUCER_BATCH_SIZE);
        return options;
    }
}
