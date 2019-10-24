package org.jmqtt.store;

import org.jmqtt.common.bean.Message;

import java.util.Collection;

/**
 * cleansession message. 离线消息存储
 */
public interface OfflineMessageStore {

    void clearOfflineMsgCache(String clientId);

    boolean containOfflineMsg(String clientId);

    /**
     * 暂存离线消息
     *  
     * @param clientId
     * @param message
     * @return
     */
    boolean addOfflineMessage(String clientId, Message message);

    /**
     * 获取客户端的所有离线消息
     *  
     * @param clientId
     * @return
     */
    Collection<Message> getAllOfflineMessage(String clientId);

}
