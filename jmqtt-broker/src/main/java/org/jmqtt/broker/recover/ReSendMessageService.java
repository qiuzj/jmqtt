package org.jmqtt.broker.recover;

import io.netty.handler.codec.mqtt.MqttPublishMessage;
import org.jmqtt.remoting.session.ClientSession;
import org.jmqtt.common.bean.Message;
import org.jmqtt.common.bean.MessageHeader;
import org.jmqtt.common.helper.ThreadFactoryImpl;
import org.jmqtt.common.log.LoggerName;
import org.jmqtt.remoting.session.ConnectManager;
import org.jmqtt.remoting.util.MessageUtil;
import org.jmqtt.store.FlowMessageStore;
import org.jmqtt.store.OfflineMessageStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.concurrent.*;
import java.util.concurrent.locks.LockSupport;

/**
 * 客户端重新连接上来时，重新发送离线消息和已发送未确认的消息
 * send offline message and flow message when client re connect and cleanSession is false
 */
public class ReSendMessageService {

    private Logger log = LoggerFactory.getLogger(LoggerName.MESSAGE_TRACE);

    private Thread thread;
    private boolean stoped = false;
    /** 待发送消息的clientId队列，说明该客户端有消息要发送 */
    private BlockingQueue<String> clients = new LinkedBlockingQueue<>();
    private OfflineMessageStore offlineMessageStore;
    private FlowMessageStore flowMessageStore;
    private int maxSize = 10000;
    private ThreadPoolExecutor sendMessageExecutor = new ThreadPoolExecutor(4,
            4,
            60,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(10000),
            new ThreadFactoryImpl("ReSendMessageThread"));

    public ReSendMessageService(OfflineMessageStore offlineMessageStore, FlowMessageStore flowMessageStore){
        this.offlineMessageStore = offlineMessageStore;
        this.flowMessageStore = flowMessageStore;
        this.thread = new Thread(new PutClient());
    }

    public boolean put(String clientId) {
        if (this.clients.size() > maxSize) {
            log.warn("ReSend message busy! the client queue size is over {}", maxSize);
            return false;
        }
        this.clients.offer(clientId);
        return true;
    }

    /**
     * 唤醒重发消息线程
     *  
     */
    public void wakeUp() {
        LockSupport.unpark(thread);
    }

    public void start(){
        thread.start();
    }

    public void shutdown(){
        if(!stoped){
            stoped = true;
        }
    }

    public boolean dispatcherMessage(String clientId, Message message) {
        ClientSession clientSession = ConnectManager.getInstance().getClient(clientId);
        // client off line again
        if (clientSession == null) {
            log.warn("The client offline again, put the message to the offline queue,clientId:{}", clientId);
            return false;
        }
        int qos = (int) message.getHeader(MessageHeader.QOS);
        int messageId = message.getMsgId();
        // 如果QoS大于0，则缓存已发送的消息。对于之前未确认消息的重发，这里将多缓存了一次，覆盖消息ID相同的消息。可以优化
        if (qos > 0) {
            flowMessageStore.cacheSendMsg(clientId, message);
        }
        MqttPublishMessage publishMessage = MessageUtil.getPubMessage(message, false, qos, messageId);
        clientSession.getCtx().writeAndFlush(publishMessage);
        return true;
    }

    /**
     * 重发消息任务
     */
    class ResendMessageTask implements Callable<Boolean> {

        private String clientId;
        public ResendMessageTask(String clientId) {
            this.clientId = clientId;
        }

        @Override
        public Boolean call() {
        	/* 重发已发送未确认的消息 */
            Collection<Message> flowMsgs = flowMessageStore.getAllSendMsg(clientId);
            for (Message message : flowMsgs) {
                if (!dispatcherMessage(clientId, message)) {
                    return false; // 如果客户端离线了，则直接返回
                }
            }
            // 发送离线消息
            if (offlineMessageStore.containOfflineMsg(clientId)) {
                Collection<Message> messages = offlineMessageStore.getAllOfflineMessage(clientId);
                for (Message message : messages) {
                    if (!dispatcherMessage(clientId, message)) {
                    	// 如果客户端离线了，则直接返回。
                    	// 可能发了一部分离线消息就离线了，这样下次这部分已重发的会再发一遍，由客户端去重。
                        return false;
                    }
                }
                // 清理客户端的所有离线缓存
                offlineMessageStore.clearOfflineMsgCache(clientId);
            }
            return true;
        }
    }

    class PutClient implements Runnable {
        @Override
        public void run() {
            while (!stoped) {
                if (clients.size() == 0) {
                    LockSupport.park(thread);
                }
                String clientId = clients.poll();
                ResendMessageTask resendMessageTask = new ResendMessageTask(clientId);
                long start = System.currentTimeMillis();
                try {
                    boolean rs = sendMessageExecutor.submit(resendMessageTask).get(2000, TimeUnit.MILLISECONDS);
                    if (!rs) {
                        log.warn("ReSend message is interrupted,the client offline again,clientId={}", clientId);
                    }
                    long cost = System.currentTimeMillis() - start;
                    log.debug("ReSend message clientId:{} cost time:{}", clientId, cost);
                } catch (Exception e) {
                    log.warn("ReSend message failure,clientId:{}", clientId);
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e1) {
                    }
                }
            }
            log.info("Shutdown re send message service success.");
        }
    }

}
