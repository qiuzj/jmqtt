package org.jmqtt.store;

import org.jmqtt.common.bean.Message;

import java.util.Collection;

/**
 * 存储与释放过程消息
 */
public interface FlowMessageStore {

    void clearClientFlowCache(String clientId);

    Message getRecMsg(String clientId, int msgId);

    /**
     * 缓存PUBREC消息
     *  
     * @param clientId
     * @param message
     * @return
     */
    boolean cacheRecMsg(String clientId, Message message);

    /**
     * 删除缓存中的PUBREC缓存消息
     *  
     * @param clientId
     * @param msgId
     * @return
     */
    Message releaseRecMsg(String clientId, int msgId);

    /**
     * 缓存已发送的消息
     *  
     * @param clientId
     * @param message
     * @return
     */
    boolean cacheSendMsg(String clientId, Message message);

    Collection<Message> getAllSendMsg(String clientId);

    /**
     * 删除缓存中的"已发送的消息"
     *  
     * @param clientId
     * @param msgId
     * @return
     */
    boolean releaseSendMsg(String clientId, int msgId);

    /**
     * 缓存中是否存在该"已发送的消息"
     *  
     * @param clientId
     * @param msgId
     * @return
     */
    boolean containSendMsg(String clientId, int msgId);

}
