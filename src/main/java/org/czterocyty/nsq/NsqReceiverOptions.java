package org.czterocyty.nsq;

import com.github.brainlag.nsq.NSQConfig;
import com.github.brainlag.nsq.lookup.DefaultNSQLookup;

import java.util.Objects;

public final class NsqReceiverOptions {

    private DefaultNSQLookup lookup = new DefaultNSQLookup();
    private String topic;
    private String channel;
    private NSQConfig config;
    private boolean stopOnConnectionError = false;

    public NsqReceiverOptions lookupAddress(String address, int port) {
        lookup.addLookupAddress(address, port);
        return this;
    }

    public NsqReceiverOptions topic(String topic) {
        this.topic = topic;
        return this;
    }

    public NsqReceiverOptions channel(String channel) {
        this.channel = channel;
        return this;
    }

    public NsqReceiverOptions config(NSQConfig config) {
        this.config = config;
        return this;
    }

    public NsqReceiverOptions stopOnConnectionError() {
        this.stopOnConnectionError = true;
        return this;
    }

    public boolean isComplete() {
        return !lookup.getLookupAddresses().isEmpty()
                && topic != null
                && channel != null;
    }

    DefaultNSQLookup getLookup() {
        return lookup;
    }

    String getTopic() {
        return topic;
    }

    String getChannel() {
        return channel;
    }

    NSQConfig getConfig() {
        return this.config == null ? new NSQConfig() : this.config;
    }

    boolean isStopOnConnectionError() {
        return stopOnConnectionError;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NsqReceiverOptions that = (NsqReceiverOptions) o;
        return stopOnConnectionError == that.stopOnConnectionError &&
                Objects.equals(lookup, that.lookup) &&
                Objects.equals(topic, that.topic) &&
                Objects.equals(channel, that.channel) &&
                Objects.equals(config, that.config);
    }

    @Override
    public int hashCode() {

        return Objects.hash(lookup, topic, channel, config, stopOnConnectionError);
    }
}
