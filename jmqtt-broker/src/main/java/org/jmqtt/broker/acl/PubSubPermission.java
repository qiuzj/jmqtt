package org.jmqtt.broker.acl;

/**
 * publish messages and subscribe topic permisson
 */
public interface PubSubPermission {

    /**
     * verfy the clientId whether can publish message to the topic
     */
    boolean publishVerfy(String clientId, String topic);

    /**
     * verfy the clientId whether can subscribe the topic.
     * 什么情况下不允许订阅该主题？
     */
    boolean subscribeVerfy(String clientId, String topic);

}
