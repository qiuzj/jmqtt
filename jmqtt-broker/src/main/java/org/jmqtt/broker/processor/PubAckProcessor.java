package org.jmqtt.broker.processor;

import org.jmqtt.common.log.LoggerName;
import org.jmqtt.remoting.netty.RequestProcessor;
import org.jmqtt.remoting.util.MessageUtil;
import org.jmqtt.remoting.util.NettyUtil;
import org.jmqtt.store.FlowMessageStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.mqtt.MqttMessage;

/**
 * 客户端接收到服务端发送的消息后，服务端确认消息已被发送，删除消息缓存.
 */
public class PubAckProcessor implements RequestProcessor {

    private Logger log = LoggerFactory.getLogger(LoggerName.MESSAGE_TRACE);
    private FlowMessageStore flowMessageStore;

    public PubAckProcessor(FlowMessageStore flowMessageStore) {
        this.flowMessageStore = flowMessageStore;
    }

    @Override
    public void processRequest(ChannelHandlerContext ctx, MqttMessage mqttMessage) {
        String clientId = NettyUtil.getClientId(ctx.channel());
        int messageId = MessageUtil.getMessageId(mqttMessage);
        log.debug("[PubAck] -> Recieve PubAck message,clientId={},msgId={}", clientId, messageId);

        /* 删除缓存消息 */
        if (!flowMessageStore.containSendMsg(clientId, messageId)) {
            log.warn("[PubAck] -> The message is not cached in Flow,clientId={},msgId={}", clientId, messageId);
            return;
        }
        flowMessageStore.releaseSendMsg(clientId, messageId);
    }
}
