package com.lealone.plugins.bench.http.http1;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.util.CharsetUtil;

public class Http1Client {

    static final boolean SSL = System.getProperty("ssl") != null;
    static String url = "/test";// "/examples/servlets/servlet/HelloWorldExample";//
    static String host = "localhost";
    static int port = 8080;
    private static int size2 = 32 * 1024;
    private static byte[] bytes = new byte[size2];
    static {
        for (int i = 0; i < size2; i++) {
            bytes[i] = (byte) i;
        }
    }

    public static void main2(String[] args) throws Exception {
        ByteBuffer gb = ByteBuffer.allocate(size2);
        for (int j = 0; j < 100; j++) {
            long t1 = System.currentTimeMillis();
            for (int i = 0; i < 100000; i++) {
                gb.put(ByteBuffer.wrap(bytes));
                gb.clear();
                gb.put(ByteBuffer.wrap(bytes));
                gb.clear();
                gb.put(ByteBuffer.wrap(bytes));
                gb.clear();
            }
            System.out.println("time: " + (System.currentTimeMillis() - t1) + " ms");
        }
    }

    public static void main(String[] args) throws Exception {
        final SslContext sslCtx;
        if (SSL) {
            sslCtx = SslContextBuilder.forClient().trustManager(InsecureTrustManagerFactory.INSTANCE)
                    .build();
            port = 8443;
        } else {
            sslCtx = null;
        }

        EventLoopGroup group = new MultiThreadIoEventLoopGroup(NioIoHandler.newFactory());
        try {
            Bootstrap b = new Bootstrap();
            b.group(group);
            b.channel(NioSocketChannel.class);
            b.handler(new ChannelInitializer<Channel>() {
                @Override
                protected void initChannel(Channel ch) {
                    ChannelPipeline p = ch.pipeline();
                    if (sslCtx != null) {
                        p.addLast(sslCtx.newHandler(ch.alloc()));
                    }
                    p.addLast(new MyByteToMessageDecoder());
                    p.addLast(new HttpClientCodec());
                    p.addLast(new HttpClientInboundHandler());
                }
            });

            Channel c = b.connect(host, port).sync().channel();
            for (int i = 1, size = 2 * 10; i <= size; i++) {
                // System.out.println("loop: " + i);
                run(c);
            }
            // long t1 = System.currentTimeMillis();
            HttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, url);
            request.headers().set(HttpHeaderNames.HOST, host);
            request.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE);
            c.writeAndFlush(request);
            c.closeFuture().sync();
            // System.out.println("close time: " + (System.currentTimeMillis() - t1) + " ms");
        } finally {
            group.shutdownGracefully();
        }
    }

    public static int size = 5 * 10000;
    public static int batch = 1000;

    public static void run(Channel c) throws Exception {
        long t1 = System.currentTimeMillis();
        for (int i = 1, s = size / batch; i <= s; i++) {
            runBatch(c);
        }
        MyByteToMessageDecoder d = (MyByteToMessageDecoder) c.pipeline().first();
        System.out.println("http request count: " + size + " byte count: " + d.count.get() + " time: "
                + (System.currentTimeMillis() - t1) + " ms");
        d.count.set(0);
    }

    public static void runBatch(Channel c) throws Exception {
        // long t1 = System.currentTimeMillis();
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger batchCounter = new AtomicInteger(batch);
        HttpClientInboundHandler handler = (HttpClientInboundHandler) c.pipeline().last();
        handler.batchCounter = batchCounter;
        handler.latch = latch;
        for (int i = 0; i < batch; i++) {
            HttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, url);
            request.headers().set(HttpHeaderNames.HOST, host);
            request.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
            c.writeAndFlush(request);
            // ChannelFuture f = c.writeAndFlush(request);
            // // 这几个方法都不会同步等待http响应结果，发送完请求就成功
            // f.get();
            // f.awaitUninterruptibly();
            // f.sync();
        }
        latch.await();
        // System.out.println("time: " + (System.currentTimeMillis() - t1));
    }

    public static class MyByteToMessageDecoder extends ByteToMessageDecoder {
        AtomicLong count = new AtomicLong();

        @Override
        protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object input) throws Exception {
            ByteBuf in = (ByteBuf) input;
            count.addAndGet(in.writerIndex());
            ctx.fireChannelRead(in);
        }
    }

    public static class HttpClientInboundHandler extends SimpleChannelInboundHandler<HttpObject> {

        AtomicInteger count = new AtomicInteger();
        long time = System.currentTimeMillis();
        AtomicInteger batchCounter;
        CountDownLatch latch;

        @Override
        public void channelRead0(ChannelHandlerContext ctx, HttpObject msg) {
            if (msg instanceof HttpContent) {
                if (msg instanceof LastHttpContent) {
                    if (batchCounter.decrementAndGet() == 0)
                        latch.countDown();
                    int c = count.incrementAndGet();
                    if (c == 5000) {
                        long time2 = System.currentTimeMillis();
                        // System.out.println("http request count: " + c + " time: " + (time2 - time) + " ms");
                        time = time2;
                        count.set(0);
                    }
                    // System.out.println("count: " + count.incrementAndGet());
                }
                return;
            }
        }

        public void channelRead00(ChannelHandlerContext ctx, HttpObject msg) {
            if (msg instanceof HttpContent) {
                System.out.println("count: " + count.incrementAndGet());
                return;
            }
            if (msg instanceof HttpResponse) {
                HttpResponse response = (HttpResponse) msg;

                System.err.println("STATUS: " + response.status());
                System.err.println("VERSION: " + response.protocolVersion());
                System.err.println();

                if (!response.headers().isEmpty()) {
                    for (CharSequence name : response.headers().names()) {
                        for (CharSequence value : response.headers().getAll(name)) {
                            System.err.println("HEADER: " + name + " = " + value);
                        }
                    }
                    System.err.println();
                }

                if (HttpUtil.isTransferEncodingChunked(response)) {
                    System.err.println("CHUNKED CONTENT {");
                } else {
                    System.err.println("CONTENT {");
                }
            }
            if (msg instanceof HttpContent) {
                HttpContent content = (HttpContent) msg;

                System.err.print(content.content().toString(CharsetUtil.UTF_8));
                System.err.flush();

                if (content instanceof LastHttpContent) {
                    System.err.println("} END OF CONTENT");
                    ctx.close();
                }
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            cause.printStackTrace();
            ctx.close();
        }
    }
}
