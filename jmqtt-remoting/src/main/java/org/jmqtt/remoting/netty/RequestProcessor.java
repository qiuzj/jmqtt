package org.jmqtt.remoting.netty;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.mqtt.MqttMessage;

/**
 * 请求报文处理接口
 */
public interface RequestProcessor {

    /**
     * 请求消息处理方法.<br>
     * handle mqtt message processor
     */
    void processRequest(ChannelHandlerContext ctx, MqttMessage mqttMessage);
}
