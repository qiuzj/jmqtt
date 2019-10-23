package org.jmqtt.remoting.util;

import io.netty.channel.Channel;
import io.netty.util.AttributeKey;

public class NettyUtil {

    private static final String CLIENT_ID = "clientId";

    private static final AttributeKey<String> clientIdAttr = AttributeKey.valueOf(CLIENT_ID);

    /**
     * 将连接Channel与客户端标识进行关联
     *  
     * @author sam.qiu  
     * @param channel
     * @param clientId
     */
    public static final void setClientId(Channel channel, String clientId){
        channel.attr(clientIdAttr).set(clientId);
    }

    /**
     * 通过连接Channel获取关联的客户端标识
     *  
     * @author sam.qiu  
     * @param channel
     * @return
     */
    public static final String getClientId(Channel channel){
        return channel.attr(clientIdAttr).get();
    }
}
