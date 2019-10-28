package org.jmqtt.store;

import org.jmqtt.common.bean.Message;

public interface WillMessageStore {

	/**
	 * 获取客户端的遗嘱消息
	 *  
	 * @param clientId
	 * @return
	 */
    Message getWillMessage(String clientId);

    /**
     * 客户端是否有遗嘱消息
     *  
     * @param clientId
     * @return
     */
    boolean hasWillMessage(String clientId);

    void storeWillMessage(String clientId, Message message);

    Message removeWillMessage(String clientId);
}
