package org.jmqtt.remoting.netty;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;

import org.jmqtt.common.config.NettyConfig;
import org.jmqtt.common.helper.MixAll;
import org.jmqtt.common.helper.Pair;
import org.jmqtt.common.helper.ThreadFactoryImpl;
import org.jmqtt.common.log.LoggerName;
import org.jmqtt.remoting.RemotingServer;
import org.jmqtt.remoting.netty.codec.ByteBuf2WebSocketEncoder;
import org.jmqtt.remoting.netty.codec.WebSocket2ByteBufDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttEncoder;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.timeout.IdleStateHandler;

/**
 * 负责的Netty启动、关闭
 */
public class NettyRemotingServer implements RemotingServer {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.REMOTING);
    /** Netty配置信息 */
    private NettyConfig nettyConfig;
    /** bossGroup */
    private EventLoopGroup selectorGroup;
    /** workerGroup */
    private EventLoopGroup ioGroup;
    /** Netty IO模型Channel实现类 */
    private Class<? extends ServerChannel> clazz;
    /** 报文处理器列表. 格式：<报文类型, 处理器对象> */
    private Map<MqttMessageType, Pair<RequestProcessor, ExecutorService>> processorTable;
    /** Netty事件执行器 */
    private NettyEventExecutor nettyEventExecutor;

    public NettyRemotingServer(NettyConfig nettyConfig, ChannelEventListener listener) {
        this.nettyConfig = nettyConfig;
        this.processorTable = new HashMap<>();
        this.nettyEventExecutor = new NettyEventExecutor(listener);

        if (!nettyConfig.isUseEpoll()) { // Nio
            selectorGroup = new NioEventLoopGroup(nettyConfig.getSelectorThreadNum(),
                    new ThreadFactoryImpl("SelectorEventGroup"));
            ioGroup = new NioEventLoopGroup(nettyConfig.getIoThreadNum(),
                    new ThreadFactoryImpl("IOEventGroup"));
            clazz = NioServerSocketChannel.class;
        } else { // Epoll
            selectorGroup = new EpollEventLoopGroup(nettyConfig.getSelectorThreadNum(),
                    new ThreadFactoryImpl("SelectorEventGroup"));
            ioGroup = new EpollEventLoopGroup(nettyConfig.getIoThreadNum(),
                    new ThreadFactoryImpl("IOEventGroup"));
            clazz = EpollServerSocketChannel.class;
        }
    }

    @Override
    public void start() {
        // Netty event executor start
        this.nettyEventExecutor.start();

        // start TCP 1883 server
        startTcpServer();

        // start Websocket server
        if (nettyConfig.isStartWebsocket()) {
            startWebsocketServer();
        }
    }

    /**
     * 启动Websocket服务
     * 
     */
    private void startWebsocketServer(){
        ServerBootstrap bootstrap = new ServerBootstrap();
        bootstrap.group(selectorGroup, ioGroup)
                .channel(clazz)
                .option(ChannelOption.SO_BACKLOG, nettyConfig.getTcpBackLog())
                .childOption(ChannelOption.TCP_NODELAY, nettyConfig.isTcpNoDelay())
                .childOption(ChannelOption.SO_SNDBUF, nettyConfig.getTcpSndBuf())
                .option(ChannelOption.SO_RCVBUF, nettyConfig.getTcpRcvBuf())
                .option(ChannelOption.SO_REUSEADDR, nettyConfig.isTcpReuseAddr())
                .childOption(ChannelOption.SO_KEEPALIVE, nettyConfig.isTcpKeepAlive())
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel socketChannel) throws Exception {
                        ChannelPipeline pipeline = socketChannel.pipeline();
                        pipeline.addLast("idleStateHandler", new IdleStateHandler(0, 0, 60))
                                .addLast("nettyConnectionManager", new NettyConnectHandler(nettyEventExecutor))
                                
                                .addLast("httpCodec", new HttpServerCodec())
                                .addLast("aggregator", new HttpObjectAggregator(65535))
                                .addLast("webSocketHandler", new WebSocketServerProtocolHandler("/mqtt", MixAll.MQTT_VERSION_SUPPORT))
                                .addLast("webSocket2ByteBufDecoder", new WebSocket2ByteBufDecoder())
                                .addLast("byteBuf2WebSocketEncoder", new ByteBuf2WebSocketEncoder())
                                
                                .addLast("mqttEncoder", MqttEncoder.INSTANCE)
                                .addLast("mqttDecoder", new MqttDecoder(nettyConfig.getMaxMsgSize()))
                                .addLast("nettyMqttHandler", new NettyMqttHandler());
                    }
                });
        if (nettyConfig.isPooledByteBufAllocatorEnable()) {
            bootstrap.childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
        }
        try {
            ChannelFuture future = bootstrap.bind(nettyConfig.getWebsocketPort()).sync();
            log.info("Start webSocket server success,port = {}", nettyConfig.getWebsocketPort());
        } catch (InterruptedException ex) {
            log.error("Start webSocket server failure.cause={}", ex);
        }
    }

    /**
     * 启动Netty TCP服务
     *  
     */
    private void startTcpServer(){
        ServerBootstrap bootstrap = new ServerBootstrap();
        bootstrap.group(selectorGroup, ioGroup)
                .channel(clazz)
                .option(ChannelOption.SO_BACKLOG, nettyConfig.getTcpBackLog())
                .childOption(ChannelOption.TCP_NODELAY, nettyConfig.isTcpNoDelay())
                .childOption(ChannelOption.SO_SNDBUF, nettyConfig.getTcpSndBuf())
                .option(ChannelOption.SO_RCVBUF, nettyConfig.getTcpRcvBuf())
                .option(ChannelOption.SO_REUSEADDR, nettyConfig.isTcpReuseAddr())
                .childOption(ChannelOption.SO_KEEPALIVE, nettyConfig.isTcpKeepAlive())
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel socketChannel) throws Exception {
                        ChannelPipeline pipeline = socketChannel.pipeline();
                        pipeline.addLast("idleStateHandler", new IdleStateHandler(60, 0, 0)) // 读空闲60s触发空闲事件，客户端连接上来后，如果传了keepAlive时间则会替换掉该Handler
                                .addLast("mqttEncoder", MqttEncoder.INSTANCE)
                                .addLast("mqttDecoder", new MqttDecoder(nettyConfig.getMaxMsgSize()))
                                .addLast("nettyConnectionManager", new NettyConnectHandler(nettyEventExecutor))
                                .addLast("nettyMqttHandler", new NettyMqttHandler());
                    }
                });
        if (nettyConfig.isPooledByteBufAllocatorEnable()) {
            bootstrap.childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
        }
        try {
            ChannelFuture future = bootstrap.bind(nettyConfig.getTcpPort()).sync();
            log.info("Start tcp server success,port = {}", nettyConfig.getTcpPort());
        } catch (InterruptedException ex) {
            log.error("Start tcp server failure.cause={}", ex);
        }
    }
	
    @Override
    public void shutdown() {
        if (selectorGroup != null) {
            selectorGroup.shutdownGracefully();
        }
        if (ioGroup != null) {
            ioGroup.shutdownGracefully();
        }
    }

    /**
     * 注册报文处理器. 先将处理器和线程池绑定在一起，作为一个对象，再将对象与报文类型关联起来并进行缓存.
     *
     * @param mqttType 报文类型. 共有14种，服务端只需要处理其中的10种（除了CONNACK、SUBACK、UNSUBACK和PINGRESP）.
     * @param processor 报文类型mqttType对应的处理器
     * @param executorService 执行processor处理器对应的线程池
     */
    public void registerProcessor(MqttMessageType mqttType, RequestProcessor processor, ExecutorService executorService) {
        this.processorTable.put(mqttType, new Pair<>(processor, executorService));
    }

    /**
     * MQTT报文处理器
     *  
     */
    class NettyMqttHandler extends ChannelInboundHandlerAdapter {

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object obj) {
            MqttMessage mqttMessage = (MqttMessage) obj;
            if (mqttMessage != null && mqttMessage.decoderResult().isSuccess()) {
                // 报文类型
                MqttMessageType messageType = mqttMessage.fixedHeader().messageType();
                log.debug("[Remoting] -> receive mqtt code,type:{}", messageType.value());

                // 根据报文类型，获得对应的处理器和线程池
                Pair<RequestProcessor, ExecutorService> pair = processorTable.get(messageType);
                
                Runnable runnable = new Runnable() {
                    @Override
                    public void run() {
                    	// 使用报文类型对应的RequestProcessor处理消息
						pair.getObject1().processRequest(ctx, mqttMessage);
                    }
                };
                
                try {
                	// 使用报文类型对应的线程池执行处理逻辑（RequestProcessor.processRequest）
                    pair.getObject2().submit(runnable);
                } catch (RejectedExecutionException ex) {
                    log.warn("Reject mqtt request, cause={}", ex.getMessage());
                }
            } else {
                ctx.close(); // 如果遇到异常报文，则关闭连接.
            }
        }

    }

}
