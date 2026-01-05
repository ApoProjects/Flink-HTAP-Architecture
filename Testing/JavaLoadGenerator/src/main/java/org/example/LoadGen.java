package org.example;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class LoadGen {

    static class Product {
        final int productId;
        final String name;
        final double price;
        Product(int productId, String name, double price) {
            this.productId = productId;
            this.name = name;
            this.price = price;
        }
    }

    static final Product[] PRODUCTS = new Product[]{
            new Product(1, "Yoghurt", 1.2),
            new Product(2, "Milk", 1.0),
            new Product(3, "Icecream", 2.3)
    };


    // Arguments to be passed into the generator. Change rate if you are running the experiments and possibly duration and warmup depending on your setup
    static class Args {
        String bootstrap = "localhost:29092";
        String topic = "cart-events";
        int users = 50_000;
        int threads = 8;
        long rate = 100;              // total events/sec across all threads (0 = unlimited)
        int durationSec = 70;       // measured duration
        int warmupSec = 10;          // warmup not counted
        String acks = "0";
        int lingerMs = 10;
        String compression = "lz4";
        boolean keyByUserId = true;
        double addRatio = 0.8;
    }

    public static void main(String[] argv) throws Exception {
        Args args = parseArgsOrDefaults(argv);

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, args.bootstrap);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        props.put(ProducerConfig.ACKS_CONFIG, args.acks);
        props.put(ProducerConfig.LINGER_MS_CONFIG, Integer.toString(args.lingerMs));
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, args.compression);

        // throughput-oriented buffers/batching
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(256 * 1024));
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, Long.toString(1024L * 1024 * 1024)); // 1GB
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false");

        KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(props);

        ExecutorService pool = Executors.newFixedThreadPool(args.threads);

        // Total target rate split across threads
        final long perThreadRate = (args.rate > 0) ? Math.max(1, args.rate / args.threads) : 0;

        // measurement control
        final long warmupEndNs = System.nanoTime() + (long) args.warmupSec * 1_000_000_000L;
        final long measureEndNs = warmupEndNs + (long) args.durationSec * 1_000_000_000L;

        // each worker returns its counted sends (only during measurement window)
        Future<Long>[] results = new Future[args.threads];

        System.out.printf(
                "Starting loadgen: topic=%s bootstrap=%s threads=%d users=%d rate=%s/s warmup=%ds measure=%ds%n",
                args.topic, args.bootstrap, args.threads, args.users,
                (args.rate == 0 ? "unlimited" : Long.toString(args.rate)),
                args.warmupSec, args.durationSec
        );

        for (int t = 0; t < args.threads; t++) {
            final int tid = t;
            results[t] = pool.submit(() -> runWorker(
                    tid, producer, args, perThreadRate, warmupEndNs, measureEndNs
            ));
        }

        // Wait until measurement end (workers will stop themselves)
        long totalSent = 0;
        for (Future<Long> f : results) {
            totalSent += f.get();
        }

        pool.shutdown();
        producer.flush();
        producer.close(Duration.ofSeconds(10));

        double avgRate = totalSent / (double) args.durationSec;
        System.out.printf("RESULT: sent=%d  avg_rate=%.0f events/s (measured for %ds)%n",
                totalSent, avgRate, args.durationSec);
    }

    static long runWorker(
            int tid,
            KafkaProducer<byte[], byte[]> producer,
            Args args,
            long perThreadRate,
            long warmupEndNs,
            long measureEndNs
    ) {
        Random rnd = new Random(12345 + tid);

        long localCountMeasured = 0;

        // pacing per thread (optional)
        long nextNs = System.nanoTime();

        while (true) {
            long now = System.nanoTime();
            if (now >= measureEndNs) break;

            if (perThreadRate > 0) {
                long intervalNs = 1_000_000_000L / perThreadRate;
                if (now < nextNs) {
                    // tiny backoff to reduce CPU burn; does not create batching
                    Thread.onSpinWait(); // Java 9+
                    continue;
                }
                nextNs += intervalNs;
            }

            int uid = 1 + rnd.nextInt(args.users);
            String userName = "User" + uid;

            Product p = PRODUCTS[rnd.nextInt(PRODUCTS.length)];

            boolean isAdd = rnd.nextDouble() < args.addRatio;
            String action = isAdd ? "ADD" : "REMOVE";
            int delta = isAdd ? 1 : -1;

            // REQUIRED JSON format (compact)
            String json = "{\"userId\":" + uid +
                    ",\"userName\":\"" + userName +
                    "\",\"productId\":" + p.productId +
                    ",\"name\":\"" + p.name +
                    "\",\"unitPrice\":" + p.price +
                    ",\"delta\":" + delta +
                    ",\"action\":\"" + action + "\"}";

            byte[] value = json.getBytes(StandardCharsets.UTF_8);
            byte[] key = args.keyByUserId ? Integer.toString(uid).getBytes(StandardCharsets.UTF_8) : null;

            producer.send(new ProducerRecord<>(args.topic, key, value));

            // count only after warmup
            if (now >= warmupEndNs) {
                localCountMeasured++;
            }
        }

        return localCountMeasured;
    }

    static Args parseArgsOrDefaults(String[] argv) {
        Args a = new Args();

        // Press Play: defaults already set in Args fields.
        if (argv == null || argv.length == 0) return a;

        for (int i = 0; i < argv.length; i++) {
            String s = argv[i];
            String v = (i + 1 < argv.length) ? argv[i + 1] : null;

            switch (s) {
                case "--bootstrap": a.bootstrap = v; i++; break;
                case "--topic": a.topic = v; i++; break;
                case "--users": a.users = Integer.parseInt(v); i++; break;
                case "--threads": a.threads = Integer.parseInt(v); i++; break;
                case "--rate": a.rate = Long.parseLong(v); i++; break;
                case "--duration": a.durationSec = Integer.parseInt(v); i++; break;
                case "--warmup": a.warmupSec = Integer.parseInt(v); i++; break;
                case "--acks": a.acks = v; i++; break;
                case "--linger-ms": a.lingerMs = Integer.parseInt(v); i++; break;
                case "--compression": a.compression = v; i++; break;
                case "--key-by-user": a.keyByUserId = true; break;
                case "--add-ratio": a.addRatio = Double.parseDouble(v); i++; break;
                default: break;
            }
        }
        return a;
    }
}