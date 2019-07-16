package in.gore.kafka.streams.processor;

import in.gore.kafka.streams.constants.Constants;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class Main {

    public static void main(String[] args) {

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-starter-app");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(StreamsConfig.CLIENT_ID_CONFIG, "client");
        // start with latest. If set to "earliest", we will start reading from the beginning of the topic if there is no
        // committed offset for the consumer.
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // timestamp extractor
        //config.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, MyTimestampExtractor.class);

        StoreBuilder<KeyValueStore<String, String>> countStoreSupplier = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(Constants.STORE_NAME),
                Serdes.String(),
                Serdes.String())
                .withLoggingDisabled(); // disable backing up the store to a changelog topic

        Topology topology = new Topology();
        topology.addSource("Source", Constants.SOURCE_TOPIC);
        topology.addSource("Source2", Constants.SOURCE_TOPIC2);
        topology.addProcessor("Processor1", () -> new HelloWorldProcessor(), "Source");
        topology.addProcessor("Processor2", () -> new TopicProcessor(), "Source2");
        topology.addStateStore(countStoreSupplier, "Processor2");


        // This statement will not work. You can "add" a state store only once.
//        topology.addStateStore(countStoreSupplier, "Processor2");

        // If you want to share the state store across processors, you need to use the API below.
        topology.connectProcessorAndStateStores("Processor1", Constants.STORE_NAME);

        // destination
        topology.addSink("Sink", Constants.DESTINATION_TOPIC, "Processor1");

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
