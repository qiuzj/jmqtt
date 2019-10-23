package org.jmqtt.broker.dispatcher;

import org.jmqtt.common.bean.Message;

/**
 * 消息分发接口
 *  
 * @version
 */
public interface MessageDispatcher {

	/**
	 * 启动消费和发送线程
	 *  
	 */
    void start();

    /**
     * 关闭消费和发送线程
     *  
     */
    void shutdown();

    /**
     * 将消息添加到发送队列
     *  
     * @param message
     * @return
     */
    boolean appendMessage(Message message);

}
