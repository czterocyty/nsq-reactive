package org.czterocyty.nsq;

import com.github.brainlag.nsq.NSQConsumer;
import com.github.brainlag.nsq.NSQMessage;
import reactor.core.publisher.Flux;

public class NsqReceiver {

    public static Flux<NSQMessage> receive(NsqReceiverOptions options) {
        return Flux.create(sink -> {
            NSQConsumer consumer = new NSQConsumer(options.getLookup(),
                    options.getTopic(),
                    options.getChannel(),
                    sink::next,
                    options.getConfig(),
                    sink::error,
                    (e, errorType) -> {
                        if (options.isStopOnConnectionError()) {
                            sink.error(e);
                        }
                    });

            sink.onDispose(consumer::shutdown);

            consumer.start();
        });
    }
}
