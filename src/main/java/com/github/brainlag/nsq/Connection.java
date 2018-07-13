package com.github.brainlag.nsq;

import com.github.brainlag.nsq.callbacks.NSQErrorCallback;
import com.github.brainlag.nsq.exceptions.NSQException;
import com.github.brainlag.nsq.exceptions.NoConnectionsException;
import com.github.brainlag.nsq.frames.ErrorFrame;
import com.github.brainlag.nsq.frames.MessageFrame;
import com.github.brainlag.nsq.frames.NSQFrame;
import com.github.brainlag.nsq.frames.ResponseFrame;
import com.github.brainlag.nsq.netty.NSQClientInitializer;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.AttributeKey;
import org.apache.logging.log4j.LogManager;

import java.net.InetSocketAddress;
import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

public class Connection {
    public static final byte[] MAGIC_PROTOCOL_VERSION = "  V2".getBytes();
    public static final AttributeKey<Connection> STATE =
            AttributeKey.valueOf("Connection.state");
    private final ServerAddress address;
    private final Channel channel;
    private NSQConsumer consumer = null;
    private NSQErrorCallback errorCallback = null;
    // This functions as a producer mutex
    private final BlockingQueue<NSQCommand> requests = new ArrayBlockingQueue<>(1, true);
    // Should only need to hold a single response per request but ¯\_(ツ)_/¯
    private final BlockingQueue<NSQFrame> responses = new ArrayBlockingQueue<>(10, true);
    private static EventLoopGroup defaultGroup = null;
    private final EventLoopGroup eventLoopGroup;
    private final NSQConfig config;

    public static final long HEARTBEAT_MAX_INTERVAL = 1L * 60L * 1000L;//default one minute
    private volatile AtomicReference<Long> lastHeartbeatSuccess = new AtomicReference<Long>(System.currentTimeMillis());


    public Connection(final ServerAddress serverAddress, final NSQConfig config) throws NoConnectionsException, UnknownHostException {
        this.address = serverAddress;
        this.config = config;
        final Bootstrap bootstrap = new Bootstrap();
        eventLoopGroup = (config.getEventLoopGroup() == null) ? getDefaultGroup() : config.getEventLoopGroup();
        LogManager.getLogger(this).info("Using {} EventLoopGroup",
                                        (config.getEventLoopGroup() == null) ? "default" : "configured");
        bootstrap.group(eventLoopGroup);
        bootstrap.channel(NioSocketChannel.class);
        bootstrap.handler(new NSQClientInitializer());
        // Start the connection attempt.
        // FORCE ipv4 for now
        final ChannelFuture future = bootstrap.connect(new InetSocketAddress(Inet4Address.getByName(serverAddress.getHost()),
                                                                             serverAddress.getPort()));

        // Wait until the connection attempt succeeds or fails.
        channel = future.awaitUninterruptibly().channel();
        if (!future.isSuccess()) {
            throw new NoConnectionsException("Could not connect to server", future.cause());
        }
        LogManager.getLogger(this).info("Created connection: {}", serverAddress.toString());
        this.channel.attr(STATE).set(this);
        final ByteBuf buf = Unpooled.buffer();
        buf.writeBytes(MAGIC_PROTOCOL_VERSION);
        channel.write(buf);
        channel.flush();

        // identify
        final NSQCommand ident = NSQCommand.identify(config.toString().getBytes());

        try {
            final NSQFrame response = commandAndWait(ident);
            if (response != null) {
                LogManager.getLogger(this).info("Server identification: {}", ((ResponseFrame) response).getMessage());
            }
        } catch (final TimeoutException e) {
            LogManager.getLogger(this).error("Creating connection timed out", e);
            close();
        }
    }

    private EventLoopGroup getDefaultGroup() {
        if (defaultGroup == null) {
            defaultGroup = new NioEventLoopGroup();
        }
        return defaultGroup;
    }

    public boolean isConnected() {
        return channel.isActive();
    }

    public boolean isRequestInProgress() {
        return !requests.isEmpty();
    }

    public boolean isHeartbeatStatusOK() {
        return (System.currentTimeMillis() - lastHeartbeatSuccess.get()) <= HEARTBEAT_MAX_INTERVAL;
    }

    public void incoming(final NSQFrame frame) {
        if (frame instanceof ResponseFrame) {
            if ("_heartbeat_".equals(((ResponseFrame) frame).getMessage())) {
                heartbeat();
                return;
            } else {
                if (isRequestInProgress()) {
                    try {
                        responses.put(frame);
                    } catch (final InterruptedException e) {
                        LogManager.getLogger(this).error("Incoming response: thread was interrupted, probably shutting down", e);
                        close();
                    }
                }
                else {
                    LogManager.getLogger(this).warn("Ignore unmatched incoming response {}",
						    ((ResponseFrame) frame).getMessage());
                }
                return;
            }
        }

        if (frame instanceof ErrorFrame) {
            if (errorCallback != null) {
                errorCallback.error(NSQException.of((ErrorFrame) frame));
            }
            LogManager.getLogger(this).warn("Adding ErrorFrame to responses");
            if (isRequestInProgress()) {
                try {
                    responses.put(frame);
                } catch (final InterruptedException e) {
                    LogManager.getLogger(this).error("Incoming error: thread was interrupted, probably shutting down", e);
                    close();
                }
            }
            else {
                LogManager.getLogger(this).warn("Unmatched incoming error {}",
                                                ((ErrorFrame) frame).getErrorMessage());
            }
            return;
        }

        if (frame instanceof MessageFrame) {
            final MessageFrame msg = (MessageFrame) frame;

            final NSQMessage message = new NSQMessage();
            message.setAttempts(msg.getAttempts());
            message.setConnection(this);
            message.setId(msg.getMessageId());
            message.setMessage(msg.getMessageBody());
            message.setTimestamp(new Date(TimeUnit.NANOSECONDS.toMillis(msg.getTimestamp())));
            consumer.processMessage(message);
            return;
        }
        LogManager.getLogger(this).warn("Unknown frame type: " + frame);
    }


    private void heartbeat() {
        LogManager.getLogger(this).trace("HEARTBEAT!");
        command(NSQCommand.nop());
        lastHeartbeatSuccess.getAndSet(System.currentTimeMillis());
    }

    public void setErrorCallback(final NSQErrorCallback callback) {
        errorCallback = callback;
    }

    public void close() {
        LogManager.getLogger(this).info("Closing connection: {}", this);
        channel.disconnect();
    }

    public NSQFrame commandAndWait(final NSQCommand command) throws TimeoutException {
        try {
            requests.put(command);

            responses.clear(); // clear the response queue
            final ChannelFuture fut = command(command);

            if (!fut.await(15, TimeUnit.SECONDS)) {
                throw new TimeoutException("await command: " + command + " timedout");
            }

            final NSQFrame frame = responses.take();
            requests.poll(); // let other threads request
            return frame;
        } catch (final InterruptedException e) {
            close();
            LogManager.getLogger(this).warn("Thread was interrupted!", e);
        }
        return null;
    }

    public ChannelFuture command(final NSQCommand command) {
        return channel.writeAndFlush(command);
    }

    public ServerAddress getServerAddress() {
        return address;
    }

    public NSQConfig getConfig() {
        return config;
    }

    public void setConsumer(final NSQConsumer consumer) {
        this.consumer = consumer;
    }
}