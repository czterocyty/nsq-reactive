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

    private static final long NSQ_MSG_TIMEOUT = 5000;

    @Test
    public void receiveSingleMessage() throws Exception {
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

    @Test
    public void receiveThenTouchThenCommit() throws Exception {
        Flux<NSQMessage> messages = NsqReceiver.receive(new NsqReceiverOptions()
                .lookupAddress(Nsq.getNsqLookupdHost(), 4161)
                .topic(TOPIC_NAME)
                .channel("channel")
                .stopOnConnectionError());

        Flux<NSQMessage> messageProcessedFiveTimes = Flux.interval(
                Duration.ofMillis(NSQ_MSG_TIMEOUT / 5 + 500))
            .take(5)
            .zipWith(messages, (left, right) -> right)
            .doOnNext(NSQMessage::touch);

        produceMessage();

        StepVerifier.create(messageProcessedFiveTimes)
                .thenRequest(5)
                .expectNextMatches(msg -> {
                    return "test-one-message".equals(new String(msg.getMessage()))
                        && msg.getAttempts() == 1;
                })
                .consumeNextWith(NSQMessage::finished)
                .thenCancel()
                .verify(Duration.ofSeconds(10));
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