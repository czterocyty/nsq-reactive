package org.czterocyty.nsq;

import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class NsqReceiverOptionsTest {

    @Test
    public void isComplete() {
        NsqReceiverOptions options = new NsqReceiverOptions()
                .channel("channel")
                .topic("topic")
                .lookupAddress("localhost", 4161);

        assertTrue(options.isComplete());
    }

    @Test
    public void isNotComplete() {
        NsqReceiverOptions options = new NsqReceiverOptions()
                .channel("channel")
                .topic("topic");

        Assert.assertFalse(options.isComplete());
    }

    @Test
    public void emptyConfigIsAlwaysReturned() {
        NsqReceiverOptions options = new NsqReceiverOptions();

        assertNotNull(options.getConfig());
    }
}