package org.jmqtt.store.memory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.jmqtt.common.bean.Message;
import org.jmqtt.store.FlowMessageStore;

public class DefaultFlowMessageStore implements FlowMessageStore {
    // 服务端与发送端交互时使用.
    // QoS 2：发送端->[发送]->服务端. 收到PUBLISH时缓存，收到PUBREL时删除.
	/** 缓存发布消息为QoS2的消息. 服务端中转QoS 2的消息，在收到Publish包时，会先存储到这里，等待发送端的PubRel确认. */
    private Map<String/*clientId*/, ConcurrentHashMap<Integer/*msgId*/, Message>> recCache = new ConcurrentHashMap<>();

    // 服务端与接收端交互时使用.
    // QoS > 0：服务端->[发送]->接收端. 发送前暂存，收到接收端确认后删除.
    // PUBLISH出去时（的前一步）缓存，收到PUBACK时、收到PUBREC发出PUBREL后、收到PUBCOMP时删除.
    /** 已发送消息的缓存，在发送前先缓存起来. 发布消息为QoS>0时使用，收到PUBACK时使用 */
    private Map<String/*clientId*/, ConcurrentHashMap<Integer/*msgId*/, Message>> sendCache = new ConcurrentHashMap<>();

    @Override
    public void clearClientFlowCache(String clientId) {
        this.recCache.remove(clientId);
        this.sendCache.remove(clientId);
    }

    @Override
    public Message getRecMsg(String clientId, int msgId) {
        return recCache.get(clientId).get(msgId);
    }

    @Override
    public boolean cacheRecMsg(String clientId, Message message) {
        if (!recCache.containsKey(clientId)) {
            synchronized (recCache) {
                if (!recCache.containsKey(clientId)) {
                    recCache.put(clientId, new ConcurrentHashMap<Integer, Message>());
                }
            }
        }
        this.recCache.get(clientId).put(message.getMsgId(), message);
        return true;
    }

    @Override
    public Message releaseRecMsg(String clientId, int msgId) {
        return this.recCache.get(clientId).remove(msgId);
    }

    @Override
    public boolean cacheSendMsg(String clientId, Message message) {
        // 初始化客户端的缓存Map
        if (!sendCache.containsKey(clientId)) {
            synchronized (sendCache) {
                if (!sendCache.containsKey(clientId)) {
                    this.sendCache.put(clientId, new ConcurrentHashMap<>());
                }
            }
        }
        // 缓存即将发送的消息
        this.sendCache.get(clientId).put(message.getMsgId(), message);
        return true;
    }

    @Override
    public Collection<Message> getAllSendMsg(String clientId) {
        if (sendCache.containsKey(clientId)) {
            return sendCache.get(clientId).values();
        }
        return new ArrayList<>();
    }

    @Override
    public boolean releaseSendMsg(String clientId, int msgId) {
        this.sendCache.get(clientId).remove(msgId);
        return true;
    }

    @Override
    public boolean containSendMsg(String clientId, int msgId) {
        return this.sendCache.get(clientId).contains(msgId);
    }
}