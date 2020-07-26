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
     * 缓存已发送的消息. 只要是发给客户端的消息，并且QoS>0，则需要调用该方法进行缓存.
     * <pre>
     * 调用来源
     * 正常发送消息：DefaultDispatcherMessage.AsyncDispatcher.run()
     * 客户端上线，重新发送消息：ReSendMessageService.dispatcherMessage()
     * 分发retain消息给新订阅者：SubscribeProcessor.dispatcherRetainMessage()
     * </pre>
     *  
     * @param clientId
     * @param message
     * @return
     */
    boolean cacheSendMsg(String clientId, Message message);

    /**
     * 获取所有消息
     * <pre>
     * 调用来源
     * 重发已发送但未确认的消息ReSendMessageService.ResendMessageTask
     * </pre>
     *  
     * @param clientId
     * @return
     */
    Collection<Message> getAllSendMsg(String clientId);

    /**
     * 删除缓存中的"已发送的消息". 收到PubAck包时删除消息
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
