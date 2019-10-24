package org.jmqtt.broker.dispatcher;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.jmqtt.broker.subscribe.SubscriptionMatcher;
import org.jmqtt.common.bean.Message;
import org.jmqtt.common.bean.MessageHeader;
import org.jmqtt.common.bean.Subscription;
import org.jmqtt.common.helper.RejectHandler;
import org.jmqtt.common.helper.ThreadFactoryImpl;
import org.jmqtt.common.log.LoggerName;
import org.jmqtt.remoting.session.ClientSession;
import org.jmqtt.remoting.session.ConnectManager;
import org.jmqtt.remoting.util.MessageUtil;
import org.jmqtt.store.FlowMessageStore;
import org.jmqtt.store.OfflineMessageStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.handler.codec.mqtt.MqttPublishMessage;

public class DefaultDispatcherMessage implements MessageDispatcher {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.MESSAGE_TRACE);
    private boolean stoped = false;
    /** 待发送的消息队列 */
    private static final BlockingQueue<Message> messageQueue = new LinkedBlockingQueue<>(100000);
    /** 消息发送线程池 */
    private ThreadPoolExecutor pollThread;
    private int pollThreadNum;
    private SubscriptionMatcher subscriptionMatcher;
    private FlowMessageStore flowMessageStore;
    private OfflineMessageStore offlineMessageStore;

    public DefaultDispatcherMessage(int pollThreadNum, SubscriptionMatcher subscriptionMatcher, FlowMessageStore flowMessageStore, OfflineMessageStore offlineMessageStore){
        this.pollThreadNum = pollThreadNum;
        this.subscriptionMatcher = subscriptionMatcher;
        this.flowMessageStore = flowMessageStore;
        this.offlineMessageStore = offlineMessageStore;
    }

    @Override
    public void start() {
    	// 初始化消息发送线程池
        this.pollThread = new ThreadPoolExecutor(pollThreadNum,
                pollThreadNum,
                60 * 1000,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(100000),
                new ThreadFactoryImpl("pollMessage2Subscriber"),
                new RejectHandler("pollMessage", 100000));

        // 不断消费messageQueue，并按批提交到pollThread进行发送
        new Thread(new Runnable() {
            @Override
            public void run() {
                int waitTime = 100;
                while (!stoped) {
                    try {
                        List<Message> messageList = new ArrayList<>(32);
                        for (int i = 0; i < 32; i++) {
                            Message message = messageQueue.poll(waitTime, TimeUnit.MILLISECONDS);
                            if (Objects.nonNull(message)) {
                                messageList.add(message);
                                waitTime = 100;
                            } else {
                                waitTime = 3000; // 如果没有待发送的消息，则每从队列获取的超时时间为3s
                                break;
                            }
                        }
                        // 每个发送任务最大32条消息
                        if (messageList.size() > 0) {
                            AsyncDispatcher dispatcher = new AsyncDispatcher(messageList);
                            pollThread.submit(dispatcher);
                        }
                    } catch (InterruptedException e) {
                        log.warn("poll message wrong.");
                    }
                }
            }
        }).start();
    }

    @Override
    public boolean appendMessage(Message message) {
        boolean isNotFull = messageQueue.offer(message);
        if (!isNotFull) {
            log.warn("[PubMessage] -> the buffer queue is full");
        }
        return isNotFull;
    }

    @Override
    public void shutdown(){
        this.stoped = true;
        this.pollThread.shutdown();
    }

    /**
     * 异步发布一批消息. 如果客户端不在线，则进行离线存储.
     *  
     */
    class AsyncDispatcher implements Runnable {

        private List<Message> messages;
        AsyncDispatcher(List<Message> messages) {
            this.messages = messages;
        }

        @Override
        public void run() {
            if (Objects.nonNull(messages)) {
                try {
                	// 循环将本批消息逐条发送到相关订阅者
                    for (Message message : messages) {
                    	// 根据topic找到所有订阅记录
                        Set<Subscription> subscriptions = subscriptionMatcher.match((String) message.getHeader(MessageHeader.TOPIC));
                        // 循环将当前消息发送给每个订阅者
                        for (Subscription subscription : subscriptions) {
                            String clientId = subscription.getClientId();
                            // 订阅者的客户端会话
                            ClientSession clientSession = ConnectManager.getInstance().getClient(subscription.getClientId());
                            // 客户端在线则立刻发送
                            if (ConnectManager.getInstance().containClient(clientId)) {
                            	// 获取最小的QoS=min(消息携带的QoS, 订阅记录的QoS)
                                int qos = MessageUtil.getMinQos((int) message.getHeader(MessageHeader.QOS), subscription.getQos());
                                int messageId = clientSession.generateMessageId();
                                // 更新消息的QoS
                                message.putHeader(MessageHeader.QOS, qos);
                                message.setMsgId(messageId);
                                // QoS大于0时，缓存已发送的消息
                                if (qos > 0) {
                                    flowMessageStore.cacheSendMsg(clientId, message);
                                }
                                MqttPublishMessage publishMessage = MessageUtil.getPubMessage(message, false, qos, messageId);
                                // 将"发布消息"发送给订阅者
                                clientSession.getCtx().writeAndFlush(publishMessage);
                                
                            // 客户端不在线则暂存到离线存储
                            } else {
                                offlineMessageStore.addOfflineMessage(clientId, message);
                            }
                        }
                    }
                } catch(Exception ex) {
                    log.warn("Dispatcher message failure,cause={}", ex);
                }
            }
        }

    }
}
