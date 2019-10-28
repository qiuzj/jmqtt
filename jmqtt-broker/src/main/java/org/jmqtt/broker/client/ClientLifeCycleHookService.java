package org.jmqtt.broker.client;

import io.netty.channel.Channel;
import org.apache.commons.lang3.StringUtils;
import org.jmqtt.common.bean.Message;
import org.jmqtt.remoting.netty.ChannelEventListener;
import org.jmqtt.broker.dispatcher.MessageDispatcher;
import org.jmqtt.remoting.util.NettyUtil;
import org.jmqtt.store.WillMessageStore;

/**
 * 客户端连接生命周期处理服务. 对应于see NettyEventType的4种事件
 *  
 * @version
 */
public class ClientLifeCycleHookService implements ChannelEventListener {

    private WillMessageStore willMessageStore;
    private MessageDispatcher messageDispatcher;

    public ClientLifeCycleHookService(WillMessageStore willMessageStore, MessageDispatcher messageDispatcher){
        this.willMessageStore = willMessageStore;
        this.messageDispatcher = messageDispatcher;
    }

    @Override
    public void onChannelConnect(String remoteAddr, Channel channel) {
    }

    @Override
    public void onChannelClose(String remoteAddr, Channel channel) {
        String clientId = NettyUtil.getClientId(channel);
        if (StringUtils.isNotEmpty(clientId)) {
            if (willMessageStore.hasWillMessage(clientId)) {
                Message willMessage = willMessageStore.getWillMessage(clientId);
                // 客户端连接关闭时，发布遗嘱消息
                messageDispatcher.appendMessage(willMessage);
            }
        }
    }

    @Override
    public void onChannelIdle(String remoteAddr, Channel channel) {

    }

    @Override
    public void onChannelException(String remoteAddr, Channel channel) {

    }
}
