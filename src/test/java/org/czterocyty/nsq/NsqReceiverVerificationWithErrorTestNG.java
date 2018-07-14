package org.czterocyty.nsq;

import com.github.brainlag.nsq.NSQMessage;
import com.github.brainlag.nsq.Nsq;
import org.reactivestreams.Publisher;

public class NsqReceiverVerificationWithErrorTestNG extends NsqReceiverVerification {

    @Override
    public Publisher<NSQMessage> createFailedPublisher() {
        NsqReceiverOptions options = new NsqReceiverOptions()
                .lookupAddress(Nsq.getNsqLookupdHost(), 2161)
                .topic(TOPIC_NAME)
                .channel("channel")
                .stopOnConnectionError();
        return NsqReceiver.receive(options)
                .doOnNext(NSQMessage::finished);
    }
}
