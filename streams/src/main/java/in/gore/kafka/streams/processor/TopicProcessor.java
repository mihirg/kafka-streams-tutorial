package in.gore.kafka.streams.processor;

import in.gore.kafka.streams.constants.Constants;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueStore;

public class TopicProcessor implements Processor<String, String> {

    private ProcessorContext processorContext;
    private KeyValueStore<String, String> keyValueStore;

    @Override
    public void init(ProcessorContext context) {
        processorContext = context;
        keyValueStore = (KeyValueStore<String, String>)context.getStateStore(Constants.STORE_NAME);
        context.schedule(1000, PunctuationType.STREAM_TIME, (timestamp) -> {

            System.out.println("punctuate called on processor 2");

            // TODO : Is this required?
           processorContext.commit();
        });
    }

    @Override
    public void process(String key, String value) {
        keyValueStore.put(key, value);

        //TODO: Is this required?
        processorContext.commit();
    }

    @Override
    public void close() {

    }
}
