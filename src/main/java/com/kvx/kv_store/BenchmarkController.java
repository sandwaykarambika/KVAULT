package com.kvx.kv_store;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@RestController
@RequestMapping("/internal/benchmark")
public class BenchmarkController {

    private static final Logger log = LoggerFactory.getLogger(BenchmarkController.class);

    @PostMapping
    public ResponseEntity<Map<String, Object>> run(
            @RequestParam(defaultValue = "1000") int ops,
            @RequestParam(defaultValue = "100") int threads,
            @RequestParam(defaultValue = "8080") int port) throws InterruptedException {

        log.info("Benchmark starting: {} ops, {} virtual threads, target :{}", ops, threads, port);

        String baseUrl = "http://localhost:" + port + "/api/kv/";
        HttpClient client = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .build();

        List<Long> latencies = Collections.synchronizedList(new ArrayList<>());
        AtomicInteger successes = new AtomicInteger();
        AtomicInteger failures = new AtomicInteger();

        // Use virtual threads (Java 21)
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            CountDownLatch latch = new CountDownLatch(ops);
            AtomicLong seq = new AtomicLong();

            long startTime = System.nanoTime();

            for (int i = 0; i < ops; i++) {
                executor.submit(() -> {
                    try {
                        long id = seq.incrementAndGet();
                        String key = "bench-" + id;
                        String url = baseUrl + key;

                        long t0 = System.nanoTime();

                        HttpRequest putReq = HttpRequest.newBuilder()
                                .uri(URI.create(url))
                                .header("Content-Type", "text/plain")
                                .PUT(HttpRequest.BodyPublishers.ofString("value-" + id))
                                .build();
                        HttpResponse<String> putResp = client.send(putReq, HttpResponse.BodyHandlers.ofString());

                        HttpRequest getReq = HttpRequest.newBuilder()
                                .uri(URI.create(url))
                                .GET()
                                .build();
                        HttpResponse<String> getResp = client.send(getReq, HttpResponse.BodyHandlers.ofString());

                        long dt = System.nanoTime() - t0;
                        latencies.add(dt / 1000); // microseconds

                        if (putResp.statusCode() == 200 && getResp.statusCode() == 200) {
                            successes.incrementAndGet();
                        } else {
                            failures.incrementAndGet();
                        }
                    } catch (Exception e) {
                        failures.incrementAndGet();
                    } finally {
                        latch.countDown();
                    }
                });
            }

            latch.await();
            long totalNs = System.nanoTime() - startTime;
            double totalSec = totalNs / 1e9;

            // Sort latencies for percentiles
            Collections.sort(latencies);

            Map<String, Object> result = new HashMap<>();
            result.put("ops", ops);
            result.put("threads", threads);
            result.put("successes", successes.get());
            result.put("failures", failures.get());
            result.put("totalSeconds", Math.round(totalSec * 100) / 100.0);
            result.put("throughputOpsPerSec", Math.round(ops / totalSec));
            result.put("latency_p50_us", percentile(latencies, 50));
            result.put("latency_p95_us", percentile(latencies, 95));
            result.put("latency_p99_us", percentile(latencies, 99));
            result.put("latency_max_us", latencies.isEmpty() ? 0 : latencies.get(latencies.size() - 1));

            log.info("Benchmark done: {} ops/sec, p50={}us, p99={}us",
                    result.get("throughputOpsPerSec"), result.get("latency_p50_us"), result.get("latency_p99_us"));

            return ResponseEntity.ok(result);
        }
    }

    private long percentile(List<Long> sorted, int pct) {
        if (sorted.isEmpty()) return 0;
        int idx = (int) Math.ceil((pct / 100.0) * sorted.size()) - 1;
        return sorted.get(Math.max(0, Math.min(idx, sorted.size() - 1)));
    }
}