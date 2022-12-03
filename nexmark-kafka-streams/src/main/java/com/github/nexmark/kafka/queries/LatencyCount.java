package com.github.nexmark.kafka.queries;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.streams.kstream.ForeachAction;

public class LatencyCount<K, V extends TimestampFromValue<V>> implements ForeachAction<K, TimestampFromValue<V>> {
    private long[] latencies;
    private int currentPos;
    private long counter;
    private final String tag;
    private final AtomicBoolean afterWarmup = new AtomicBoolean(false);
    private BufferedWriter bw;
    private final ExecutorService es;

    public LatencyCount(final String tag, final String filename) {
        try {
            bw = new BufferedWriter(new FileWriter(filename));
        } catch (final IOException e) {
            e.printStackTrace();
        }
        latencies = new long[1024];
        currentPos = 0;
        es = Executors.newSingleThreadExecutor();
        this.tag = tag;
    }

    public void SetAfterWarmup() {
        afterWarmup.set(true);
    }

    @Override
    public void apply(final K key, final TimestampFromValue<V> value) {
        this.counter += 1;
        if (afterWarmup.get()) {
            final long ts = value.extract();
            final long lat = Instant.now().toEpochMilli() - ts;
            if (currentPos < latencies.length) {
                latencies[currentPos] = lat;
                currentPos++;
            } else {
                final String s = Arrays.toString(latencies);
                latencies = new long[1024];
                currentPos = 0;
                es.submit(() -> {
                    try {
                        bw.write(s);
                        bw.newLine();
                    } catch (final IOException e) {
                        e.printStackTrace();
                    }
                });
            }
        }
    }

    public void outputRemainingStats() {
        try {
            es.awaitTermination(60, java.util.concurrent.TimeUnit.SECONDS);
        } catch (final InterruptedException e1) {
            e1.printStackTrace();
        }
        es.shutdown();
        if (currentPos > 0) {
            final String s = Arrays.toString(Arrays.copyOfRange(latencies, 0, currentPos));
            try {
                bw.write(s);
                bw.newLine();
                bw.close();
            } catch (final IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void printCount() {
        System.out.println(tag + ": " + counter);
    }
}
