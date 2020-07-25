package org.jmqtt.store;

import org.jmqtt.common.bean.Message;

import java.util.Collection;

/**
 * 用于PUBLISH时保留消息. 仅当保留位为1，且QoS大于0、Payload有数据时才进行保留.
 */
public interface RetainMessageStore {

    /**
     * 获取所有的retain消息
     * 
     * @return
     */
    Collection<Message> getAllRetainMessage();

    /**
     * 存储retain消息
     * 
     * @param topic
     * @param message
     */
    void storeRetainMessage(String topic,Message message);

    /**
     * 移除retain消息
     * 
     * @param topic
     */
    void removeRetainMessage(String topic);
}
