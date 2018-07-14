package org.czterocyty.nsq;

import com.github.brainlag.nsq.NSQMessage;
import com.github.brainlag.nsq.NSQProducer;
import com.github.brainlag.nsq.Nsq;
import com.github.brainlag.nsq.exceptions.NSQException;
import org.reactivestreams.Publisher;
import org.reactivestreams.tck.PublisherVerification;
import org.reactivestreams.tck.TestEnvironment;

import java.util.concurrent.TimeoutException;

public abstract class NsqReceiverVerification extends PublisherVerification<NSQMessage> {
    protected static final String TOPIC_NAME = "tck";

    protected NsqReceiverVerification() {
        super(new TestEnvironment(500L, 500L, true));
    }

    protected void produceMessage(long elements) throws NSQException, TimeoutException {
        NSQProducer producer = new NSQProducer();
        producer.addAddress(Nsq.getNsqdHost(), 4150);
        producer.start();
        for (int i = 0; i < elements; i++) {
            String msg = "test-one-message-" + i;
            producer.produce(TOPIC_NAME, msg.getBytes());
        }
        producer.shutdown();
    }

    @Override
    public long maxElementsFromPublisher() {
        return publisherUnableToSignalOnComplete();
    }

    @Override
    public Publisher<NSQMessage> createPublisher(long elements) {
        if (elements > 10000) {
            elements = 10000; // we limit it
        }

        try {
            produceMessage(elements);
        } catch (Exception e) {
            throw new RuntimeException("Cannot produce messages");
        }

        NsqReceiverOptions options = new NsqReceiverOptions()
                .lookupAddress(Nsq.getNsqLookupdHost(), 4161)
                .topic(TOPIC_NAME)
                .channel("channel");
        return NsqReceiver.receive(options)
                .doOnNext(NSQMessage::finished);
    }

}
