package com.github.nexmark.kafka.queries;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.streams.kstream.ForeachAction;

public class LatencyCount<K, V extends TimestampFromValue<V>> implements ForeachAction<K, TimestampFromValue<V>> {
    long[] latencies;
    int currentPos;
    long counter;
    String tag;
    AtomicBoolean afterWarmup = new AtomicBoolean(false);
    BufferedWriter bw;
    ExecutorService es;

    public LatencyCount(String tag, String filename) {
        try {
            bw = new BufferedWriter(new FileWriter(filename));
        } catch (IOException e) {
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
    public void apply(K key, TimestampFromValue<V> value) {
        this.counter += 1;
        long ts = value.extract();
        long lat = System.currentTimeMillis() - ts;
        if (currentPos < latencies.length) {
            latencies[currentPos] = lat;
            currentPos++;
        } else {
            String s = Arrays.toString(latencies);
            latencies = new long[1024];
            currentPos = 0;
            es.execute(() -> {
                try {
                    bw.write(s);
                    bw.newLine();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }
    }

    public void outputRemainingStats() {
        try {
            es.awaitTermination(60, java.util.concurrent.TimeUnit.SECONDS);
        } catch (InterruptedException e1) {
            e1.printStackTrace();
        }
        es.shutdown();
        if (currentPos > 0) {
            String s = Arrays.toString(Arrays.copyOfRange(latencies, 0, currentPos));
            try {
                bw.write(s);
                bw.newLine();
                bw.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void printCount() {
        System.out.println(tag + ": " + counter);
    }
}
