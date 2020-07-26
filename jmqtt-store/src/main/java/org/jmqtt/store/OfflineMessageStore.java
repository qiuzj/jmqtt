package org.jmqtt.store;

import org.jmqtt.common.bean.Message;

import java.util.Collection;

/**
 * 离线消息存储.<br>
 * cleansession message.
 */
public interface OfflineMessageStore {

	/**
	 * 清理客户端的所有离线缓存
	 *  
	 * @param clientId
	 */
    void clearOfflineMsgCache(String clientId);

    /**
     * 该客户端是否有离线消息.
     *
     * @param clientId
     * @return
     */
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
