package com.github.nexmark.kafka.queries;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.Arrays;

public class LatencyHolder {
    long[] latencies;
    int currentPos;
    String fileName;
    long counter;
    BufferedWriter bw;
    ExecutorService es;
    String tag;
    AtomicBoolean afterWarmup = new AtomicBoolean(false);

    public LatencyHolder(String tag, String filename) throws IOException {
        this.tag = tag;
        this.fileName = filename; 
    }

    public void init() {
        System.out.println(tag + " latencyHolder init");
        latencies = new long[1024];
        counter = 0;
        currentPos = 0;
        try {
            bw = new BufferedWriter(new FileWriter(this.fileName));
        } catch (IOException e) {
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

    public void appendLatency(long lat) {
        counter += 1;
        if (currentPos < latencies.length) {
            latencies[currentPos] = lat;
            currentPos++;
        } else {
            String s = Arrays.toString(latencies);
            latencies = new long[1024];
            currentPos = 0;
            es.submit(() -> {
                try {
                    bw.write(s);
                    bw.newLine();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }
    }

    public void waitForFinish() {
        try {
            es.awaitTermination(60, java.util.concurrent.TimeUnit.SECONDS);
            es.shutdown();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void outputRemainingStats() {
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
}
