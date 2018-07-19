package org.czterocyty.nsq;

import com.github.brainlag.nsq.NSQMessage;
import com.github.brainlag.nsq.NSQProducer;
import com.github.brainlag.nsq.Nsq;
import com.github.brainlag.nsq.exceptions.NSQException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.concurrent.TimeoutException;

public class NsqReceiverTest {

    private static final String TOPIC_NAME = "receiver";

    private static final long NSQ_MSG_TIMEOUT = 5000;
    private static final Logger LOGGER = LogManager.getLogger(NsqReceiverTest.class);

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
                .consumeNextWith(msg -> {
                    try {
                        Assert.assertEquals("test-one-message", new String(msg.getMessage()));
                    } finally {
                        msg.finished();
                    }
                })
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

        Flux<NSQMessage> messageProcessedFiveTimes = Flux.interval(Duration.ofMillis(NSQ_MSG_TIMEOUT / 5 + 500))
            .withLatestFrom(messages, (left, right) -> {
                LOGGER.info("attempt {}", right.getAttempts());
                return right;
            })
            .doOnNext(message -> {
                LOGGER.info("Touching message");
                message.touch();
            })
            .take(5)
            .skip(4)
            .log();

        produceMessage();

        StepVerifier.create(messageProcessedFiveTimes)
                .consumeNextWith(record -> {
                    try {
                        Assert.assertEquals("test-one-message", new String(record.getMessage()));
                        Assert.assertEquals(1, record.getAttempts());
                    } finally {
                        record.finished();
                    }
                })
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