package org.jmqtt.store;

import org.jmqtt.common.bean.Subscription;

import java.util.Collection;

public interface SubscriptionStore {

	/**
	 * 保存客户端的订阅记录
	 *  
	 * @param clientId
	 * @param subscription
	 * @return
	 */
    boolean storeSubscription(String clientId, Subscription subscription);

    Collection<Subscription> getSubscriptions(String clientId);

    boolean clearSubscription(String clientId);

    boolean removeSubscription(String clientId,String topic);

}
