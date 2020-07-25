package org.jmqtt.common.bean;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * 内部消息对象，用于内部传输、存储等，对外发送的时候需要转化为MqttMessage.<br>
 * inner message transfer from MqttMessage
 */
public class Message {
	/** 消息ID. 如果是发送消息，则在发送前调用ClientSession.generateMessageId()生成. 如果是响应，则直接使用请求的消息ID. */
    private int msgId;
    /** 消息头. see MessageHeader */
    private Map<String, Object> headers;
    /** 发送端的客户端标识 */
    private String clientId;
    /** 报文类型. 使用保留值15：WILL(15) */
    private Type type;
    /** 有效负载 */
    private byte[] payload;

    public Message() {};

    public Message(Type type, Map<String,Object> headers, byte[] payload) {
        this.type = type;
        this.headers = headers;
        this.payload = payload;
    }

    public Object putHeader(String key, Object value) {
        if (headers == null) {
            headers = new HashMap<>();
        }
        return headers.put(key, value);
    }

    public Object removeHeader(String key) {
        return headers.remove(key);
    }

    public Object getHeader(String key) {
        return headers.get(key);
    }

    public int getMsgId() {
        return msgId;
    }

    public void setMsgId(int msgId) {
        this.msgId = msgId;
    }

    public Map<String, Object> getHeaders() {
        return headers;
    }

    public void setHeaders(Map<String, Object> headers) {
        this.headers = headers;
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public byte[] getPayload() {
        return payload;
    }

    public void setPayload(byte[] payload) {
        this.payload = payload;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    /**
     * mqtt message type
     */
    public enum Type{
        CONNECT(1),
        CONNACK(2),
        PUBLISH(3),
        PUBACK(4),
        PUBREC(5),
        PUBREL(6),
        PUBCOMP(7),
        SUBSCRIBE(8),
        SUBACK(9),
        UNSUBSCRIBE(10),
        UNSUBACK(11),
        PINGREQ(12),
        PINGRESP(13),
        DISCONNECT(14),
        WILL(15); // 相比MqttMessageType多了一个WILL(15)，但15在协议当中是保留字段

        private int value;

        private Type(int value) {
            this.value = value;
        }

        public int value() {
            return this.value;
        }

        public static Type valueOf(int type) {
            Type[] var1 = values();
            int var2 = var1.length;

            for(int var3 = 0; var3 < var2; ++var3) {
                Type t = var1[var3];
                if (t.value == type) {
                    return t;
                }
            }
            throw new IllegalArgumentException("unknown message type: " + type);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Message message = (Message) o;
        return msgId == message.msgId &&
                Objects.equals(headers, message.headers) &&
                Objects.equals(clientId, message.clientId) &&
                type == message.type &&
                Objects.equals(payload, message.payload);
    }

    @Override
    public int hashCode() {
        return Objects.hash(msgId, headers, clientId, type, payload);
    }

    @Override
    public String toString() {
        return "Message{" +
                "msgId=" + msgId +
                ", headers=" + headers +
                ", clientId=" + clientId +
                ", type=" + type +
                ", payload=" + payload +
                '}';
    }
}
