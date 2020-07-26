package org.jmqtt.broker.processor;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.mqtt.MqttMessage;
import org.jmqtt.store.FlowMessageStore;
import org.jmqtt.broker.dispatcher.MessageDispatcher;
import org.jmqtt.common.bean.Message;
import org.jmqtt.common.log.LoggerName;
import org.jmqtt.remoting.netty.RequestProcessor;
import org.jmqtt.remoting.session.ConnectManager;
import org.jmqtt.remoting.util.MessageUtil;
import org.jmqtt.remoting.util.NettyUtil;
import org.jmqtt.remoting.util.RemotingHelper;
import org.jmqtt.store.RetainMessageStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * 消息接收方处理PUBREL报文（ 发布释放，QoS 2，第二步）
 *  
 * @version
 */
public class PubRelProcessor extends AbstractMessageProcessor implements RequestProcessor {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.MESSAGE_TRACE);

    private FlowMessageStore flowMessageStore;

    public PubRelProcessor(MessageDispatcher messageDispatcher, FlowMessageStore flowMessageStore, RetainMessageStore retainMessageStore) {
        super(messageDispatcher,retainMessageStore);
        this.flowMessageStore = flowMessageStore;
    }

    @Override
    public void processRequest(ChannelHandlerContext ctx, MqttMessage mqttMessage) {
        String clientId = NettyUtil.getClientId(ctx.channel());
        int messageId = MessageUtil.getMessageId(mqttMessage);
        
        if (ConnectManager.getInstance().containClient(clientId)) {
            Message message = flowMessageStore.releaseRecMsg(clientId, messageId);
            // 收到（客户端）发送端的PUBREC报文，再开始发送消息. 假设没收到，那要么消息一直存储要么丢弃...坑爹了？
            // 对于QoS 2，服务端中转消息时才有这第第二步ACK，说明发送端已经知道服务端接收完消息了.
            // 如果发送端没有收到PubComp，是否重发？如果不重发，可能服务端根本没收到PubRec，于是消息没发；
            // 如果重发，但服务端已经将消息发送出去，再来的消息需要考虑去重。
            if (Objects.nonNull(message)) {
                super.processMessage(message);
            } else {
                log.warn("[PubRelMessage] -> the message is not exist,clientId={},messageId={}.", clientId, messageId);
            }
            MqttMessage pubComMessage = MessageUtil.getPubComMessage(messageId);
            ctx.writeAndFlush(pubComMessage);
        } else {
            log.warn("[PubRelMessage] -> the client：{} disconnect to this server.", clientId);
            RemotingHelper.closeChannel(ctx.channel());
        }
    }
}
