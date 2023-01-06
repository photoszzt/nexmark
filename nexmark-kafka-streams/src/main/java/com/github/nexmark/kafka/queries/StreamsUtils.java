package com.github.nexmark.kafka.queries;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.streams.StreamsConfig;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import static com.github.nexmark.kafka.queries.Constants.NUM_STATS;

public class StreamsUtils {
    public static Properties getExactlyOnceStreamsConfig(final String bootstrapServer, final int duration,
                                                         final int flushms, final boolean disableCache,
                                                         final boolean disableBatching) {
        System.out.println("using exactly once config");
        final Properties props = new Properties();
        if (disableCache) {
            props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");
        }
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);

        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 3);
        props.put(StreamsConfig.topicPrefix(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG), 3);
        props.put(StreamsConfig.producerPrefix(ProducerConfig.ACKS_CONFIG), "all");
        final int batchSize = disableBatching ? 0 : 128 * 1024;
        final int flush = disableBatching ? 0 : flushms;
        props.put(StreamsConfig.producerPrefix(ProducerConfig.BATCH_SIZE_CONFIG), batchSize);
        props.put(StreamsConfig.producerPrefix(ProducerConfig.LINGER_MS_CONFIG), flush);
        props.put(StreamsConfig.producerPrefix(ProducerConfig.RETRY_BACKOFF_MS_CONFIG), 20);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, Integer.toString(flushms));
        props.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 0);
        props.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);

        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, Integer.toString(flushms));
        props.put(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG, "INFO");
        props.put(StreamsConfig.METRICS_NUM_SAMPLES_CONFIG, "100"); // metrics computed over 10 s
        props.put(StreamsConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG, "100"); // every 100 ms
        return props;
    }

    public static Properties getAtLeastOnceStreamsConfig(final String bootstrapServer, final int duration, final int flushms,
                                                         final boolean disableCache, final boolean disableBatching) {
        System.out.println("using at least once config");
        final Properties props = new Properties();
        if (disableCache) {
            props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");
        }
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 3);
        props.put(StreamsConfig.topicPrefix(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG), 3);
        final int batchSize = disableBatching ? 0 : 128 * 1024;
        final int flush = disableBatching ? 0 : flushms;
        props.put(StreamsConfig.producerPrefix(ProducerConfig.BATCH_SIZE_CONFIG), batchSize);
        props.put(StreamsConfig.producerPrefix(ProducerConfig.LINGER_MS_CONFIG), flush);
        props.put(StreamsConfig.producerPrefix(ProducerConfig.RETRY_BACKOFF_MS_CONFIG), 20);
        props.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, Integer.toString(flushms));
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, Integer.toString(flushms));
        props.put(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG, "INFO");
        props.put(StreamsConfig.METRICS_NUM_SAMPLES_CONFIG, "100"); // metrics computed over 10 s
        props.put(StreamsConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG, "100"); // every 100 ms
        return props;
    }

    public static void createTopic(final String bootstrapServer, final Collection<NewTopic> nps) {
        final Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        try (Admin admin = Admin.create(props)) {
            final CreateTopicsResult res = admin.createTopics(nps);
            final Map<String, KafkaFuture<Void>> futures = res.values();
            nps.forEach(np -> {
                final KafkaFuture<Void> future = futures.get(np.name());
                try {
                    future.get();
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
            });
        }
    }

    public static void appendLat(final ArrayList<Long> lat, final long ts, final String tag) {
        if (lat.size() == NUM_STATS) {
            System.out.println("{\"" + tag + "\": " + lat + "}");
            lat.clear();
        }
        lat.add(ts);
    }

    public static void printRemaining(final ArrayList<Long> lat, final String tag) {
        if (lat.size() > 0) {
            System.out.println("{\"" + tag + "\": " + lat + "}");
        }
    }
}
