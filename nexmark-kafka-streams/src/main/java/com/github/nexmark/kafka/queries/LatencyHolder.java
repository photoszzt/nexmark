package com.github.nexmark.kafka.queries;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.Arrays;

public class LatencyHolder {
    private long[] latencies;
    private int currentPos;
    private final String fileName;
    private long counter;
    private BufferedWriter bw;
    private ExecutorService es;
    private final String tag;
    private AtomicBoolean afterWarmup = new AtomicBoolean(false);

    public LatencyHolder(final String tag, final String filename) throws IOException {
        this.tag = tag;
        this.fileName = filename; 
    }

    public String tag() {
        return tag;
    }

    public void init() {
        System.out.println(tag + " latencyHolder init");
        latencies = new long[1024];
        counter = 0;
        currentPos = 0;
        try {
            bw = new BufferedWriter(new FileWriter(this.fileName));
        } catch (final IOException e) {
            e.printStackTrace();
        }
        es = Executors.newSingleThreadExecutor();
    }

    public long getCount() {
        return counter;
    }

    public void SetAfterWarmup() {
        afterWarmup.set(true);
    }

    public void appendLatency(final long lat) {
        counter += 1;
        if (afterWarmup.get()) {
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

    public void waitForFinish() {
        try {
            es.awaitTermination(60, java.util.concurrent.TimeUnit.SECONDS);
            es.shutdown();
        } catch (final InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void outputRemainingStats() {
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
}
