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
 * 客户端重新连接上来时，重新发送离线消息和已发送未确认的消息.<br>
 * send offline message and flow message when client re connect and cleanSession is false
 */
public class ReSendMessageService {

    private Logger log = LoggerFactory.getLogger(LoggerName.MESSAGE_TRACE);

    /** 执行PutClient任务的线程，即不断从clients中获取clienId构造重发任务提交到线程池 */
    private Thread thread;
    /** 用于控制是否从clients中poll出客户端ID的标志 */
    private boolean stoped = false;
    /** 待发送消息的clientId队列，说明该客户端有消息要发送 */
    private BlockingQueue<String> clients = new LinkedBlockingQueue<>();
    /** 离线存储 */
    private OfflineMessageStore offlineMessageStore;
    /** 待确认消息存储 */
    private FlowMessageStore flowMessageStore;
    /** 待重发客户端ID的最大数量 */
    private int maxSize = 10000;
    /** 实际执行重发消息任务的线程池 */
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

    /**
     * 将需要重发消息的客户端ID提交进来. 被org.jmqtt.broker.processor.ConnectProcessor调用，然后调用wakeUp()
     *
     * @param clientId
     * @return
     */
    public boolean put(String clientId) {
        if (this.clients.size() > maxSize) {
            log.warn("ReSend message busy! the client queue size is over {}", maxSize);
            return false;
        }
        this.clients.offer(clientId);
        return true;
    }

    /**
     * 唤醒重发消息线程. 被org.jmqtt.broker.processor.ConnectProcessor调用，先调用put()方法，再唤醒
     *  
     */
    public void wakeUp() {
        LockSupport.unpark(thread);
    }

    /**
     * 启动执行PutClient的线程
     */
    public void start(){
        thread.start();
    }

    /**
     * 停止执行PutClient的线程
     */
    public void shutdown(){
        if (!stoped) {
            stoped = true;
        }
    }

    /**
     * 实际分发消息的方法.
     * <ol>
     *     <li>获取客户端会话，并判断会话是否存在</li>
     *     <li>缓存已发送待确认消息</li>
     *     <li>构造发布消息报文，并发送给客户端</li>
     * </ol>
     *
     * @param clientId
     * @param message
     * @return
     */
    public boolean dispatcherMessage(String clientId, Message message) {
        // 1.获取客户端会话，并判断会话是否存在
        ClientSession clientSession = ConnectManager.getInstance().getClient(clientId);
        // client off line again. 获取不到客户端会话，可能客户端离线，或者客户端已经连到其他服务器了.
        // 这里需要考虑服务端集群，客户端切换到不同服务端的离线、待确认消息发送问题.
        // 如果客户端离线，其实待确认消息也变成了离线消息.
        if (clientSession == null) {
            log.warn("The client offline again, put the message to the offline queue,clientId:{}", clientId);
            return false;
        }

        // 2.缓存已发送待确认消息
        int qos = (int) message.getHeader(MessageHeader.QOS);
        int messageId = message.getMsgId();
        // 如果QoS大于0，则缓存已发送的消息。对于之前未确认消息的重发，这里将多缓存了一次，覆盖消息ID相同的消息。可以优化
        if (qos > 0) {
            flowMessageStore.cacheSendMsg(clientId, message);
        }

        // 3.构造发布消息报文，并发送给客户端
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
            // 1.重发已发送未确认的消息
        	// 1）获取客户端的所有已发送未确认的消息. 简单粗暴，但获取一批应该会比获取一条更加高效. 可能的问题是一批太大怎么办？
            Collection<Message> flowMsgs = flowMessageStore.getAllSendMsg(clientId);
            // 2）循环重发每条已发送未确认的消息
            for (Message message : flowMsgs) {
                if (!dispatcherMessage(clientId, message)) {
                    return false; // 如果客户端离线了，则直接返回
                }
            }

            // 2.发送离线消息
            if (offlineMessageStore.containOfflineMsg(clientId)) { // 客户端是否有离线消息
                // 1）获取客户端的所有离线消息. 简单粗暴，但获取一批应该会比获取一条更加高效. 可能的问题是一批太大怎么办？
                Collection<Message> messages = offlineMessageStore.getAllOfflineMessage(clientId);
                // 2）循环发送每一条消息
                for (Message message : messages) {
                    if (!dispatcherMessage(clientId, message)) {
                    	// 如果客户端离线了，则直接返回。
                    	// 可能发了一部分离线消息就离线了，这样下次这部分已重发的会再发一遍，由客户端去重。
                        return false;
                    }
                }
                // 3）清理客户端的所有离线缓存
                offlineMessageStore.clearOfflineMsgCache(clientId);
            }
            return true;
        }
    }

    /**
     * 这个线程任务逻辑非常简单
     * <ol>
     * <li>poll出待重发的客户端ID；
     * <li>然后new ResendMessageTask(clientId)扔到线程池中去执行.
     * </ol>
     */
    class PutClient implements Runnable {
        @Override
        public void run() {
            while (!stoped) {
                // 如果没有待发送的客户端信息，只挂起线程. 如果要简单的话，sleep一会儿也行.
                if (clients.size() == 0) {
                    LockSupport.park(thread);
                }

                // 取出一个待发送的客户端ID
                String clientId = clients.poll();
                // 构造重发消息任务
                ResendMessageTask resendMessageTask = new ResendMessageTask(clientId);

                long start = System.currentTimeMillis();
                try {
                    // 将任务提交到线程池
                    boolean rs = sendMessageExecutor.submit(resendMessageTask).get(2000, TimeUnit.MILLISECONDS);
                    // 客户端离线
                    if (!rs) {
                        log.warn("ReSend message is interrupted, the client offline again, clientId={}", clientId);
                    }

                    // 客户端本次重发消息总耗时
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
