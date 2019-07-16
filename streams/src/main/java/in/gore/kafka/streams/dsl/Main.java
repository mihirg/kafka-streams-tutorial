package in.gore.kafka.streams.dsl;


import in.gore.kafka.streams.constants.Constants;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;


public class Main {

    public static void main(String[] args) {

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-starter-app");
        config.put(StreamsConfig.CLIENT_ID_CONFIG, "dsl-client");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        final StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> data = builder.stream(Constants.SOURCE_TOPIC);

        // materializes the ktable, i.e. stores it underlying topic
        KTable<String, String> metadata = builder.table(Constants.SOURCE_TOPIC2, Materialized.as("metadata-store"));


        // use a kstream-ktable join. The stream looks up the key on the ktable.
        KStream<String, String> joined = data.join(metadata, (left, right) -> {
            StringBuilder sbr = new StringBuilder();
            return sbr.append(left).append("_").append(right).toString();
        });

        joined.to(Constants.DESTINATION_TOPIC);

        final Topology topology = builder.build();
        System.out.println(topology.describe());

        final KafkaStreams streams = new KafkaStreams(topology, config);

        final CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Exception exp) {

        }

        System.exit(0);

    }
}