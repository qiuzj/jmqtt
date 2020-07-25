package org.jmqtt.broker.acl;

/**
 * 发布信息、订阅主题权限校验.<br>
 * publish messages and subscribe topic permisson
 */
public interface PubSubPermission {

    /**
     * 客户端是否有权限发布消息到指定topic.<br>
     * verfy the clientId whether can publish message to the topic
     */
    boolean publishVerfy(String clientId, String topic);

    /**
     * 客户端是否有权限订阅指定topic.<br>
     * verfy the clientId whether can subscribe the topic.
     * 什么情况下不允许订阅该主题？
     */
    boolean subscribeVerfy(String clientId, String topic);

}
