package org.jmqtt.broker.dispatcher;

import org.jmqtt.common.bean.Message;

/**
 * 消息分发接口
 *  
 * @version
 */
public interface MessageDispatcher {

    void start();

    void shutdown();

    /**
     * 将消息添加到发送队列
     *  
     * @param message
     * @return
     */
    boolean appendMessage(Message message);

}
