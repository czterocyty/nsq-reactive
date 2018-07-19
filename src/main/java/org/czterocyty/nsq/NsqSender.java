package org.czterocyty.nsq;

import com.github.brainlag.nsq.NSQProducer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

public class NsqSender implements Subscriber<byte[]> {

    private static final Logger LOGGER = LogManager.getLogger(NsqSender.class);

    private Subscription subscription;

    private final String topic;
    private final NSQProducer producer;
    private boolean running = false;

    public NsqSender(NsqSenderOptions options) {
        producer = options.createNSQProducer();
        topic = options.getTopic();
    }

    @Override
    public void onSubscribe(Subscription s) {
        this.subscription = s;

        try {
            producer.start();
            running = true;

            this.subscription.request(1);
        } catch (Exception e) {
            LOGGER.warn("Cannot start NSQ", e);
            try {
                producer.shutdown();
            } finally {
                this.subscription.cancel();
                this.subscription = null;
            }
        }
    }

    @Override
    public void onNext(byte[] bytes) {
        try {
            if (running) {
                producer.produce(topic, bytes);

                this.subscription.request(1);
            }
        } catch (Exception e) {
            LOGGER.warn("Cannot produce record", e);
            running = false;
            try {
                producer.shutdown();
            } finally {
                this.subscription.cancel();
                this.subscription = null;
            }
        }
    }

    @Override
    public void onError(Throwable t) {
        shutdown();
    }

    @Override
    public void onComplete() {
        shutdown();
    }

    private void shutdown() {
        try {
            if (running) {
                running = false;
                try {
                    producer.shutdown();
                } catch (Exception e) {
                    LOGGER.warn("Cannot close producer", e);
                }
            }
        } finally {
            subscription = null;
        }
    }

}
