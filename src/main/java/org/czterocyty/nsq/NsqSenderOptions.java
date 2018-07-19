package org.czterocyty.nsq;

import com.github.brainlag.nsq.NSQProducer;

public class NsqSenderOptions {

    private final String topic;
    private final String address;
    private final int port;

    public NsqSenderOptions(String topic, String address, int port) {
        this.topic = topic;
        this.address = address;
        this.port = port;
    }

    NSQProducer createNSQProducer() {
        return new NSQProducer().addAddress(this.address, this.port);
    }

    String getTopic() {
        return topic;
    }
}
