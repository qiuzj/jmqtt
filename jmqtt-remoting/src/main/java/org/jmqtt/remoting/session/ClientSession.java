package org.jmqtt.remoting.session;

import io.netty.channel.ChannelHandlerContext;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 一个客户端会话.
 *  
 * @version
 */
public class ClientSession {
	/** 客户端标识 */
    private String clientId;
    /** 清理会话（Clean Session，第1位） */
    private boolean cleanSession;
    /** 处理客户端连接ChannelHandler对应的ChannelHandlerContext，内部维护了对应的客户端连接Channel、ChannelHandler和Pipeline */
    private transient ChannelHandlerContext ctx;

    private AtomicInteger messageIdCounter = new AtomicInteger(1);

    public ClientSession(){}

    public ClientSession(String clientId, boolean cleanSession) {
        this.clientId = clientId;
        this.cleanSession = cleanSession;
    }

    public ClientSession(String clientId, boolean cleanSession, ChannelHandlerContext ctx) {
        this.clientId = clientId;
        this.cleanSession = cleanSession;
        this.ctx = ctx;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public boolean isCleanSession() {
        return cleanSession;
    }

    public void setCleanSession(boolean cleanSession) {
        this.cleanSession = cleanSession;
    }

    public ChannelHandlerContext getCtx() {
        return ctx;
    }

    public void setCtx(ChannelHandlerContext ctx) {
        this.ctx = ctx;
    }

    /**
     * 生成消息ID. 报文标识符为2个字节，1~65535循环使用.
     *  
     * @return
     */
    public int generateMessageId() {
        int messageId = messageIdCounter.getAndIncrement();
        messageId = Math.abs(messageId % 0xFFFF); // 最大2个字节
        // 消息ID不为0
        if (messageId == 0) {
            return generateMessageId();
        }
        return messageId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ClientSession that = (ClientSession) o;
        return Objects.equals(clientId, that.clientId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(clientId);
    }
}
