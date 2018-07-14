package org.czterocyty.nsq;

import com.github.brainlag.nsq.NSQMessage;
import org.reactivestreams.Publisher;

public class NsqReceiverVerificationNoErrorTestNG extends NsqReceiverVerification {

    @Override
    public Publisher<NSQMessage> createFailedPublisher() {
        return null;
    }
}
