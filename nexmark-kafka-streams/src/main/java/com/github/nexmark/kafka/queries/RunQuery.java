package com.github.nexmark.kafka.queries;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class RunQuery {
    private static NexmarkQuery getNexmarkQuery(int queryNumber) {
        if (queryNumber == 1) {
            return new Query1();
        } else {
            System.err.println("Wrong query number: " + queryNumber);
            return null;
        }
    }

    public static void main(final String[] args) {
        if (args.length != 1) {
            System.err.println("Need to specify query number");
            System.exit(1);
        }
        int queryNumber = Integer.getInteger(args[1]);
        NexmarkQuery query = getNexmarkQuery(queryNumber);
        if (query == null) {
            System.exit(1);
        }
        StreamsBuilder builder = query.getStreamBuilder();
        Properties props = query.getProperties();

        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-pipe-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (final Throwable e) {
            e.printStackTrace();
            System.exit(1);
        }
        System.exit(0);
    }
}
