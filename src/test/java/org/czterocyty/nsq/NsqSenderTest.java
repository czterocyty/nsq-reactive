package org.czterocyty.nsq;

import com.github.brainlag.nsq.NSQMessage;
import com.github.brainlag.nsq.Nsq;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Duration;

public class NsqSenderTest {

    private static final String TOPIC = "nsq-reactive-producer";

    @Test
    public void produceThenConsumer() {
        NsqSender sender = new NsqSender(new NsqSenderOptions(TOPIC, Nsq.getNsqdHost(), 4150));

        Flux.range(1, 10)
                .map(counter -> "message-" + counter)
                .map(String::getBytes)
                .subscribe(sender);

        Flux<NSQMessage> received = NsqReceiver.receive(new NsqReceiverOptions()
                    .topic(TOPIC)
                    .channel("test")
                    .lookupAddress(Nsq.getNsqLookupdHost(), 4161))
                .doOnNext(NSQMessage::finished);

        StepVerifier.create(received)
                .expectNextCount(10)
                .expectNoEvent(Duration.ofMillis(200))
                .thenCancel()
                .verify(Duration.ofSeconds(5));
    }
}