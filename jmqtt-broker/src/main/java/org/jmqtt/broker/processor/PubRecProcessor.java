package org.jmqtt.broker.processor;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.mqtt.MqttMessage;
import org.jmqtt.store.FlowMessageStore;
import org.jmqtt.common.log.LoggerName;
import org.jmqtt.remoting.netty.RequestProcessor;
import org.jmqtt.remoting.util.MessageUtil;
import org.jmqtt.remoting.util.NettyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PubRecProcessor implements RequestProcessor {

    private Logger log = LoggerFactory.getLogger(LoggerName.MESSAGE_TRACE);
    private FlowMessageStore flowMessageStore;

    public PubRecProcessor(FlowMessageStore flowMessageStore){
        this.flowMessageStore = flowMessageStore;
    }

    @Override
    public void processRequest(ChannelHandlerContext ctx, MqttMessage mqttMessage) {
        String clientId = NettyUtil.getClientId(ctx.channel());
        int messageId = MessageUtil.getMessageId(mqttMessage);
        log.debug("[PubRec] -> Recieve PubRec message,clientId={},msgId={}",clientId,messageId);
        
        if (!flowMessageStore.containSendMsg(clientId, messageId)) {
            log.warn("[PubRec] -> The message is not cached in Flow,clientId={},msgId={}", clientId, messageId);
        }
        // 返回PUBREL报文 – 发布释放（QoS 2，第二步），然后删除缓存的消息
        MqttMessage pubRelMessage = MessageUtil.getPubRelMessage(messageId);
        ctx.writeAndFlush(pubRelMessage).addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture channelFuture) throws Exception {
                flowMessageStore.releaseSendMsg(clientId, messageId);
            }
        });
    }
}
