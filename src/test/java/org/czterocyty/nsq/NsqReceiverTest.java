package org.czterocyty.nsq;

import com.github.brainlag.nsq.NSQMessage;
import com.github.brainlag.nsq.NSQProducer;
import com.github.brainlag.nsq.Nsq;
import com.github.brainlag.nsq.exceptions.NSQException;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.util.concurrent.TimeoutException;

public class NsqReceiverTest {

    private static final String TOPIC_NAME = "receiver";

    @Test
    public void receive() throws Exception {
        Flux<NSQMessage> messages = NsqReceiver.receive(new NsqReceiverOptions()
                .lookupAddress(Nsq.getNsqLookupdHost(), 4161)
                .topic(TOPIC_NAME)
                .channel("channel"));

        produceMessage();

        StepVerifier.create(messages)
                .consumeNextWith(NSQMessage::finished)
                .expectNextMatches(msg -> "test-one-message".equals(new String(msg.getMessage())))
                .thenCancel()
                .verify();
    }

    private void produceMessage() throws NSQException, TimeoutException {
        NSQProducer producer = new NSQProducer();
        producer.addAddress(Nsq.getNsqdHost(), 4150);
        producer.start();
        String msg = "test-one-message";
        producer.produce(TOPIC_NAME, msg.getBytes());
        producer.shutdown();
    }
}