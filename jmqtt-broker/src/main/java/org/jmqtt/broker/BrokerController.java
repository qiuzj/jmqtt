package org.jmqtt.broker;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.jmqtt.broker.acl.ConnectPermission;
import org.jmqtt.broker.acl.PubSubPermission;
import org.jmqtt.broker.acl.impl.DefaultConnectPermission;
import org.jmqtt.broker.acl.impl.DefaultPubSubPermission;
import org.jmqtt.broker.client.ClientLifeCycleHookService;
import org.jmqtt.broker.dispatcher.DefaultDispatcherMessage;
import org.jmqtt.broker.dispatcher.MessageDispatcher;
import org.jmqtt.broker.processor.ConnectProcessor;
import org.jmqtt.broker.processor.DisconnectProcessor;
import org.jmqtt.broker.processor.PingProcessor;
import org.jmqtt.broker.processor.PubAckProcessor;
import org.jmqtt.broker.processor.PubCompProcessor;
import org.jmqtt.broker.processor.PubRecProcessor;
import org.jmqtt.broker.processor.PubRelProcessor;
import org.jmqtt.broker.processor.PublishProcessor;
import org.jmqtt.broker.processor.SubscribeProcessor;
import org.jmqtt.broker.processor.UnSubscribeProcessor;
import org.jmqtt.broker.recover.ReSendMessageService;
import org.jmqtt.broker.subscribe.DefaultSubscriptionTreeMatcher;
import org.jmqtt.broker.subscribe.SubscriptionMatcher;
import org.jmqtt.common.config.BrokerConfig;
import org.jmqtt.common.config.NettyConfig;
import org.jmqtt.common.config.StoreConfig;
import org.jmqtt.common.helper.MixAll;
import org.jmqtt.common.helper.RejectHandler;
import org.jmqtt.common.helper.ThreadFactoryImpl;
import org.jmqtt.common.log.LoggerName;
import org.jmqtt.remoting.netty.ChannelEventListener;
import org.jmqtt.remoting.netty.NettyRemotingServer;
import org.jmqtt.remoting.netty.RequestProcessor;
import org.jmqtt.store.AbstractMqttStore;
import org.jmqtt.store.FlowMessageStore;
import org.jmqtt.store.OfflineMessageStore;
import org.jmqtt.store.RetainMessageStore;
import org.jmqtt.store.SessionStore;
import org.jmqtt.store.SubscriptionStore;
import org.jmqtt.store.WillMessageStore;
import org.jmqtt.store.memory.DefaultMqttStore;
import org.jmqtt.store.rocksdb.RDBMqttStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.handler.codec.mqtt.MqttMessageType;

/**
 * BrokerController为初始化类，初始化所有的必备环境，其中acl，store的插件配置也必须在这里初始化
 *  
 * @version
 */
public class BrokerController {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.BROKER);

    /** 服务端配置对象 */
    private BrokerConfig brokerConfig;
    /** Netty配置对象 */
    private NettyConfig nettyConfig;
    /** 存储系统配置对象 */
    private StoreConfig storeConfig;

    /** 负责接收连接、断开连接处理逻辑的线程池 */
    private ExecutorService connectExecutor;
    /** 负责发布消息处理逻辑的线程池 */
    private ExecutorService pubExecutor;
    /** 负责接收订阅主题处理逻辑的线程池 */
    private ExecutorService subExecutor;
    /** 负责心跳请求处理逻辑的线程池 */
    private ExecutorService pingExecutor;

    /* 与上面4个线程池相对应的4个队列 */
    /** 连接断连任务队列 */
    private LinkedBlockingQueue connectQueue;
    /** 发布消息任务队列 */
    private LinkedBlockingQueue pubQueue;
    /** 订阅主题任务队列 */
    private LinkedBlockingQueue subQueue;
    /** 心跳请求任务队列 */
    private LinkedBlockingQueue pingQueue;

    /** ClientLifeCycleHookService. 连接的生命周期事件处理 */
    private ChannelEventListener channelEventListener;
    /** 负责的Netty启动、关闭 */
    private NettyRemotingServer remotingServer;
    /** 消息分发器 */
    private MessageDispatcher messageDispatcher;
    private FlowMessageStore flowMessageStore;
    /** 订阅树维护 */
    private SubscriptionMatcher subscriptionMatcher;
    private WillMessageStore willMessageStore;
    private RetainMessageStore retainMessageStore;
    private OfflineMessageStore offlineMessageStore;
    private SubscriptionStore subscriptionStore;
    private SessionStore sessionStore;
    /** 抽象存储对象，可插拔 */
    private AbstractMqttStore abstractMqttStore;
    /** 客户端连接相关权限的校验器 */
    private ConnectPermission connectPermission;
    /** 订阅、发布相关权限的校验器 */
    private PubSubPermission pubSubPermission;
    /** 离线消息发送服务 */
    private ReSendMessageService reSendMessageService;

    public BrokerController(BrokerConfig brokerConfig, NettyConfig nettyConfig,StoreConfig storeConfig){
        /* 保存配置信息 */
        this.brokerConfig = brokerConfig;
        this.nettyConfig = nettyConfig;
        this.storeConfig = storeConfig;

        /* 创建任务队列 */
        this.connectQueue = new LinkedBlockingQueue(100000);
        this.pubQueue = new LinkedBlockingQueue(100000);
        this.subQueue = new LinkedBlockingQueue(100000);
        this.pingQueue = new LinkedBlockingQueue(10000);

        // store pluggable. 初始化存储，可插拨式，选择不同的存储系统.
        {
            switch (storeConfig.getStoreType()){
                case 1:
                    this.abstractMqttStore = new RDBMqttStore(storeConfig); // rocksdb存储
                    break;
                default:
                    this.abstractMqttStore = new DefaultMqttStore(); // 内存存储
                break;
            }

            // 初始化存储对象
            try {
                this.abstractMqttStore.init();
            } catch (Exception e) {
                System.out.println("Init Store failure,exception=" + e);
                e.printStackTrace();
            }

            // 放一份存储对象的相关引用到当前对象，方便后续使用
            this.flowMessageStore = this.abstractMqttStore.getFlowMessageStore();
            this.willMessageStore = this.abstractMqttStore.getWillMessageStore();
            this.retainMessageStore = this.abstractMqttStore.getRetainMessageStore();
            this.offlineMessageStore = this.abstractMqttStore.getOfflineMessageStore();
            this.subscriptionStore = this.abstractMqttStore.getSubscriptionStore();
            this.sessionStore = this.abstractMqttStore.getSessionStore();
        }
        
        // permission pluggable. 初始化权限校验器
        {
            this.connectPermission = new DefaultConnectPermission();
            this.pubSubPermission = new DefaultPubSubPermission();
        }

        this.subscriptionMatcher = new DefaultSubscriptionTreeMatcher();
        this.messageDispatcher = new DefaultDispatcherMessage(brokerConfig.getPollThreadNum(), subscriptionMatcher, flowMessageStore, offlineMessageStore);

        this.channelEventListener = new ClientLifeCycleHookService(willMessageStore, messageDispatcher);
        this.remotingServer = new NettyRemotingServer(nettyConfig, channelEventListener);
        this.reSendMessageService = new ReSendMessageService(offlineMessageStore, flowMessageStore);

        int coreThreadNum = Runtime.getRuntime().availableProcessors();
        this.connectExecutor = new ThreadPoolExecutor(coreThreadNum*2,
                coreThreadNum*2,
                60000,
                TimeUnit.MILLISECONDS,
                connectQueue,
                new ThreadFactoryImpl("ConnectThread"),
                new RejectHandler("connect",100000));
        this.pubExecutor = new ThreadPoolExecutor(coreThreadNum*2,
                coreThreadNum*2,
                60000,
                TimeUnit.MILLISECONDS,
                pubQueue,
                new ThreadFactoryImpl("PubThread"),
                new RejectHandler("pub",100000));
        this.subExecutor = new ThreadPoolExecutor(coreThreadNum*2,
                coreThreadNum*2,
                60000,
                TimeUnit.MILLISECONDS,
                subQueue,
                new ThreadFactoryImpl("SubThread"),
                new RejectHandler("sub",100000));
        this.pingExecutor = new ThreadPoolExecutor(coreThreadNum,
                coreThreadNum,
                60000,
                TimeUnit.MILLISECONDS,
                pingQueue,
                new ThreadFactoryImpl("PingThread"),
                new RejectHandler("heartbeat",100000));
    }

    public void start(){

        // 打印所有属性配置的键值对
        MixAll.printProperties(log, brokerConfig);
        MixAll.printProperties(log, nettyConfig);
        MixAll.printProperties(log, storeConfig);

        // init and register processor. 初始化处理器，并将报文类型与处理器、线程池关联，同时注册到远程Netty服务中.
        {
            // 1.初始化处理器. 除了CONNACK、SUBACK、UNSUBACK和PINGRESP四个报文，其他14个都有相应的处理器.
            RequestProcessor connectProcessor = new ConnectProcessor(this);
            RequestProcessor disconnectProcessor = new DisconnectProcessor(this);
            RequestProcessor pingProcessor = new PingProcessor();
            RequestProcessor publishProcessor = new PublishProcessor(this);
            RequestProcessor pubRelProcessor = new PubRelProcessor(messageDispatcher, flowMessageStore, retainMessageStore);
            RequestProcessor subscribeProcessor = new SubscribeProcessor(this);
            RequestProcessor unSubscribeProcessor = new UnSubscribeProcessor(subscriptionMatcher, subscriptionStore);
            RequestProcessor pubRecProcessor = new PubRecProcessor(flowMessageStore);
            RequestProcessor pubAckProcessor = new PubAckProcessor(flowMessageStore);
            RequestProcessor pubCompProcessor = new PubCompProcessor(flowMessageStore);

            // 2.注册报文处理器. 先将处理器和线程池绑定在一起，作为一个对象，再将对象与报文类型关联起来并进行缓存.
            this.remotingServer.registerProcessor(MqttMessageType.CONNECT, connectProcessor, connectExecutor);
            this.remotingServer.registerProcessor(MqttMessageType.DISCONNECT, disconnectProcessor, connectExecutor);
            
            this.remotingServer.registerProcessor(MqttMessageType.PINGREQ, pingProcessor, pingExecutor);

            // 下面几个PUB为什么这样分组，有点不明白。既然是双向的，为什么不是都使用同一个线程池？

            this.remotingServer.registerProcessor(MqttMessageType.PUBLISH, publishProcessor, pubExecutor);
            this.remotingServer.registerProcessor(MqttMessageType.PUBACK, pubAckProcessor, pubExecutor);
            this.remotingServer.registerProcessor(MqttMessageType.PUBREL, pubRelProcessor, pubExecutor);
            
            this.remotingServer.registerProcessor(MqttMessageType.SUBSCRIBE, subscribeProcessor, subExecutor);
            this.remotingServer.registerProcessor(MqttMessageType.UNSUBSCRIBE, unSubscribeProcessor, subExecutor);
            this.remotingServer.registerProcessor(MqttMessageType.PUBREC, pubRecProcessor, subExecutor);
            this.remotingServer.registerProcessor(MqttMessageType.PUBCOMP, pubCompProcessor, subExecutor);
        }
        
        // 启动消息分发器
        this.messageDispatcher.start();
        // 启动重发线程
        this.reSendMessageService.start();
        // 启动Netty
        this.remotingServer.start();
        log.info("JMqtt Server start success and version = {}",brokerConfig.getVersion());
    }

    public void shutdown(){
        this.remotingServer.shutdown();
        this.connectExecutor.shutdown();
        this.pubExecutor.shutdown();
        this.subExecutor.shutdown();
        this.pingExecutor.shutdown();
        this.messageDispatcher.shutdown();
        this.reSendMessageService.shutdown();
        this.abstractMqttStore.shutdown();
    }

    public BrokerConfig getBrokerConfig() {
        return brokerConfig;
    }

    public NettyConfig getNettyConfig() {
        return nettyConfig;
    }

    public ExecutorService getConnectExecutor() {
        return connectExecutor;
    }

    public ExecutorService getPubExecutor() {
        return pubExecutor;
    }

    public ExecutorService getSubExecutor() {
        return subExecutor;
    }

    public ExecutorService getPingExecutor() {
        return pingExecutor;
    }

    public LinkedBlockingQueue getConnectQueue() {
        return connectQueue;
    }

    public LinkedBlockingQueue getPubQueue() {
        return pubQueue;
    }

    public LinkedBlockingQueue getSubQueue() {
        return subQueue;
    }

    public LinkedBlockingQueue getPingQueue() {
        return pingQueue;
    }

    public NettyRemotingServer getRemotingServer() {
        return remotingServer;
    }

    public MessageDispatcher getMessageDispatcher() {
        return messageDispatcher;
    }

    public FlowMessageStore getFlowMessageStore() {
        return flowMessageStore;
    }

    public SubscriptionMatcher getSubscriptionMatcher() {
        return subscriptionMatcher;
    }

    public WillMessageStore getWillMessageStore() {
        return willMessageStore;
    }

    public RetainMessageStore getRetainMessageStore() {
        return retainMessageStore;
    }

    public OfflineMessageStore getOfflineMessageStore() {
        return offlineMessageStore;
    }

    public SubscriptionStore getSubscriptionStore() {
        return subscriptionStore;
    }

    public SessionStore getSessionStore() {
        return sessionStore;
    }

    public ConnectPermission getConnectPermission() {
        return connectPermission;
    }

    public PubSubPermission getPubSubPermission() {
        return pubSubPermission;
    }

    public ReSendMessageService getReSendMessageService() {
        return reSendMessageService;
    }
}
