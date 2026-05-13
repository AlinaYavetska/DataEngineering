package ua.edu.de.streams;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

import java.util.*;
import java.util.concurrent.CountDownLatch;

/**
 * Kafka Streams application: reads trip events from "Topic1" and produces 4 aggregations
 * (keyed by trip date YYYY-MM-DD) into separate output topics.
 *
 *   a) avg-duration-per-day       — average trip duration (seconds)
 *   b) trips-count-per-day        — number of trips
 *   c) top-start-station-per-day  — most popular start station
 *   d) top3-stations-per-day      — top-3 stations counting starts AND ends
 */
public class TripsStreamsApp {

    private static final ObjectMapper M = new ObjectMapper();

    public static final String IN_TOPIC = env("INPUT_TOPIC", "Topic1");
    public static final String OUT_AVG = env("OUT_AVG", "avg-duration-per-day");
    public static final String OUT_COUNT = env("OUT_COUNT", "trips-count-per-day");
    public static final String OUT_TOP_START = env("OUT_TOP_START", "top-start-station-per-day");
    public static final String OUT_TOP3 = env("OUT_TOP3", "top3-stations-per-day");

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, env("APP_ID", "trips-streams-app"));
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                env("KAFKA_BOOTSTRAP_SERVERS", "broker1:29092,broker2:29093"));
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 2000);
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.AT_LEAST_ONCE);

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> trips = builder.stream(IN_TOPIC,
                Consumed.with(Serdes.String(), Serdes.String()));

        // Rekey by trip date (YYYY-MM-DD parsed from start_time)
        KStream<String, JsonNode> byDate = trips
                .mapValues(TripsStreamsApp::parse)
                .filter((k, v) -> v != null && v.hasNonNull("start_time"))
                .selectKey((k, v) -> tripDate(v));

        // ===== a) Average trip duration per day =====
        byDate
                .filter((d, v) -> v.hasNonNull("trip_duration"))
                .mapValues(v -> new DurationAgg(v.get("trip_duration").asLong(), 1L))
                .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<>(DurationAgg.class)))
                .reduce(DurationAgg::merge,
                        Materialized.with(Serdes.String(), new JsonSerde<>(DurationAgg.class)))
                .toStream()
                .mapValues(agg -> jsonOf("avg_duration_seconds", agg.avg(), "trips", agg.count))
                .to(OUT_AVG, Produced.with(Serdes.String(), Serdes.String()));

        // ===== b) Trips count per day =====
        byDate
                .mapValues(v -> "")
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .count(Materialized.with(Serdes.String(), Serdes.Long()))
                .toStream()
                .mapValues(c -> jsonOf("trips_count", c))
                .to(OUT_COUNT, Produced.with(Serdes.String(), Serdes.String()));

        // ===== c) Most popular START station per day =====
        byDate
                .filter((d, v) -> v.hasNonNull("start_station_name"))
                .map((d, v) -> KeyValue.pair(d + "|" + v.get("start_station_name").asText(), 1L))
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Long()))
                .reduce(Long::sum, Materialized.with(Serdes.String(), Serdes.Long()))
                .toStream()
                .map((compositeKey, count) -> {
                    String[] p = compositeKey.split("\\|", 2);
                    return KeyValue.pair(p[0], new StationCount(p[1], count));
                })
                .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<>(StationCount.class)))
                .reduce((a, b) -> b.count >= a.count ? b : a,
                        Materialized.with(Serdes.String(), new JsonSerde<>(StationCount.class)))
                .toStream()
                .mapValues(sc -> jsonOf("top_start_station", sc.station, "trips", sc.count))
                .to(OUT_TOP_START, Produced.with(Serdes.String(), Serdes.String()));

        // ===== d) Top-3 stations per day (start + end counted together) =====
        KStream<String, String> stationsExpanded = byDate.flatMap((date, v) -> {
            List<KeyValue<String, String>> out = new ArrayList<>(2);
            if (v.hasNonNull("start_station_name")) {
                out.add(KeyValue.pair(date + "|" + v.get("start_station_name").asText(), ""));
            }
            if (v.hasNonNull("end_station_name")) {
                out.add(KeyValue.pair(date + "|" + v.get("end_station_name").asText(), ""));
            }
            return out;
        });

        stationsExpanded
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .count(Materialized.with(Serdes.String(), Serdes.Long()))
                .toStream()
                .map((compositeKey, count) -> {
                    String[] p = compositeKey.split("\\|", 2);
                    return KeyValue.pair(p[0], new StationCount(p[1], count));
                })
                .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<>(StationCount.class)))
                .aggregate(Top3::new,
                        (key, sc, agg) -> agg.add(sc),
                        Materialized.with(Serdes.String(), new JsonSerde<>(Top3.class)))
                .toStream()
                .mapValues(Top3::toJson)
                .to(OUT_TOP3, Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            streams.close();
            latch.countDown();
        }));

        try {
            streams.start();
            System.out.println("[streams] running. input=" + IN_TOPIC);
            latch.await();
        } catch (Throwable t) {
            t.printStackTrace();
            System.exit(1);
        }
    }

    // ---------- helpers ----------

    static String env(String k, String d) {
        String v = System.getenv(k);
        return v == null || v.isBlank() ? d : v;
    }

    static JsonNode parse(String s) {
        try { return M.readTree(s); } catch (Exception e) { return null; }
    }

    static String tripDate(JsonNode v) {
        String t = v.get("start_time").asText();
        return t.length() >= 10 ? t.substring(0, 10) : t;
    }

    static String jsonOf(Object... kv) {
        ObjectNode n = M.createObjectNode();
        for (int i = 0; i + 1 < kv.length; i += 2) {
            String key = String.valueOf(kv[i]);
            Object val = kv[i + 1];
            if (val instanceof Long l) n.put(key, l.longValue());
            else if (val instanceof Integer ii) n.put(key, ii.intValue());
            else if (val instanceof Double d) n.put(key, d.doubleValue());
            else if (val instanceof Number num) n.put(key, num.doubleValue());
            else n.put(key, String.valueOf(val));
        }
        try { return M.writeValueAsString(n); } catch (Exception e) { return "{}"; }
    }

    // ---------- value types ----------

    public static class DurationAgg {
        public long sum;
        public long count;
        public DurationAgg() {}
        public DurationAgg(long s, long c) { sum = s; count = c; }
        public DurationAgg merge(DurationAgg o) { return new DurationAgg(sum + o.sum, count + o.count); }
        public double avg() { return count == 0 ? 0.0 : (double) sum / count; }
    }

    public static class StationCount {
        public String station;
        public long count;
        public StationCount() {}
        public StationCount(String s, long c) { station = s; count = c; }
    }

    public static class Top3 {
        public List<StationCount> top = new ArrayList<>();

        public Top3 add(StationCount sc) {
            top.removeIf(x -> x.station.equals(sc.station));
            top.add(sc);
            top.sort((a, b) -> Long.compare(b.count, a.count));
            if (top.size() > 3) top = new ArrayList<>(top.subList(0, 3));
            return this;
        }

        public String toJson() {
            ObjectNode root = M.createObjectNode();
            var arr = root.putArray("top3");
            for (StationCount sc : top) {
                ObjectNode o = M.createObjectNode();
                o.put("station", sc.station);
                o.put("count", sc.count);
                arr.add(o);
            }
            try { return M.writeValueAsString(root); } catch (Exception e) { return "{}"; }
        }
    }
}
