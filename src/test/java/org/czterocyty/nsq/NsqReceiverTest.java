package org.czterocyty.nsq;

import com.github.brainlag.nsq.NSQMessage;
import com.github.brainlag.nsq.NSQProducer;
import com.github.brainlag.nsq.Nsq;
import com.github.brainlag.nsq.exceptions.NSQException;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.concurrent.TimeoutException;

public class NsqReceiverTest {

    private static final String TOPIC_NAME = "receiver";

    @Test
    public void receive() throws Exception {
        Flux<NSQMessage> messages = NsqReceiver.receive(new NsqReceiverOptions()
                    .lookupAddress(Nsq.getNsqLookupdHost(), 4161)
                    .topic(TOPIC_NAME)
                    .channel("channel")
                .stopOnConnectionError());

        produceMessage();

        StepVerifier.create(messages)
                .thenRequest(1)
                .expectNextMatches(msg -> "test-one-message".equals(new String(msg.getMessage())))
                .consumeNextWith(NSQMessage::finished)
                .thenCancel()
                .verify(Duration.ofSeconds(5));
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