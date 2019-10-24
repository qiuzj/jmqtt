package org.jmqtt.broker.processor;

import org.jmqtt.broker.dispatcher.MessageDispatcher;
import org.jmqtt.common.bean.Message;
import org.jmqtt.common.bean.MessageHeader;
import org.jmqtt.store.RetainMessageStore;

public abstract class AbstractMessageProcessor {

    protected MessageDispatcher messageDispatcher;

    private RetainMessageStore retainMessageStore;

    public AbstractMessageProcessor(MessageDispatcher messageDispatcher,RetainMessageStore retainMessageStore){
        this.messageDispatcher = messageDispatcher;
        this.retainMessageStore = retainMessageStore;
    }

    /**
     * 发布消息. PUBLISH Qos 0、QoS1使用
     *  
     * @param message
     */
    protected void processMessage(Message message) {
    	// 1.将消息添加到发送队列
        this.messageDispatcher.appendMessage(message);
        
        boolean retain = (boolean) message.getHeader(MessageHeader.RETAIN);
        // 2.保留消息
        // 如果RETAIN为1，而QoS为0，或Payload为0字节，则丢弃之前为该主题保留的的任何消息。
        // 保留标志为1且有效载荷为零字节的PUBLISH报文，同一个主题下任何现存的保留消息必须被移除。
        // 如果RETAIN为0，则不保留当前消息，也不替换现存的保留消息。
        if (retain) {
            int qos = (int) message.getHeader(MessageHeader.QOS);
            byte[] payload = message.getPayload();
            String topic = (String) message.getHeader(MessageHeader.TOPIC);
            // qos == 0 or payload is none,then clear previous retain message
            // 如果RETAIN为1，而QoS为0，或Payload为0字节，则丢弃之前为该主题保留的的任何消息
            if (qos == 0 || payload == null || payload.length == 0) {
                this.retainMessageStore.removeRetainMessage(topic);
            } else { // 保留消息
                this.retainMessageStore.storeRetainMessage(topic, message);
            }
        }
    }

}
