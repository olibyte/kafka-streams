package myapps;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;

/**
 * Pipe
 */
public class Pipe {

    private static final String STREAMS_SHUTDOWN_HOOK = "streams-shutdown-hook";
    private static final String KAFKA_INPUT_TOPIC = "streams-plaintext-input";
    private static final String KAFKA_OUTPUT_TOPIC = "streams-pipe-output";

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-pipe");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        final StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> source = builder.stream(KAFKA_INPUT_TOPIC);
        // source.to(KAFKA_OUTPUT_TOPIC);

        source.map((k, v) -> KeyValue.pair(k, "Intercepted! " + v))
                .to(KAFKA_OUTPUT_TOPIC);

        KStream<String, Integer> transformed = source.map(
                (key, value) -> KeyValue.pair(value.toLowerCase(), value.length()));
        // transformed.to(KAFKA_OUTPUT_TOPIC);

        final Topology topology = builder.build();
        System.out.println(topology.describe());

        final KafkaStreams streams = new KafkaStreams(topology, props);
        final CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread(STREAMS_SHUTDOWN_HOOK) {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
                System.out.println("adios amigo");
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(1);
        }

        System.exit(0);
    }
}