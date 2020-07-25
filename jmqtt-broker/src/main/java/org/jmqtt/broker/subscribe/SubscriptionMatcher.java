package org.jmqtt.broker.subscribe;

import org.jmqtt.common.bean.Subscription;

import java.util.Set;

/**
 * 订阅树管理.<br>
 * Subscription tree
 */
public interface SubscriptionMatcher {

    /**
     * 添加订阅记录到订阅树中
     * 
     * @param topic 订阅的Topic
     * @param subscription 订阅对象
     * @return  true：新增的订阅，无该topic存在或者qos不同，那么必须分发retain消息
     *           false：重复订阅或订阅异常,不分发retain消息
     */
    boolean subscribe(Subscription subscription);

    boolean unSubscribe(String topic, String clientId);

    /**
     * 获取匹配topic的所有订阅者
     *  
     * @param topic
     * @return
     */
    Set<Subscription> match(String topic);

    /**
     * 如果消息的主题pubTopic，能够在已订阅的主题subTopic上得到匹配，则返回true
     *
     * @param pubTopic
     * @param subTopic
     * @return
     */
    boolean isMatch(String pubTopic, String subTopic);
}
