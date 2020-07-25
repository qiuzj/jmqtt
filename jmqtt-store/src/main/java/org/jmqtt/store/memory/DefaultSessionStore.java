package org.jmqtt.store.memory;

import org.jmqtt.store.SessionStore;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 客户端连接会话存储. 保存会话信息. 仅维护是否在线状态，即值为true or false
 */
public class DefaultSessionStore implements SessionStore {

    private Map<String/*clientId*/, Object> sessionTable = new ConcurrentHashMap<>();

    @Override
    public boolean containSession(String clientId) {
        return sessionTable.containsKey(clientId);
    }

    @Override
    public Object setSession(String clientId, Object obj) {
        return this.sessionTable.put(clientId, obj);
    }

    @Override
    public Object getLastSession(String clientId) {
        return sessionTable.get(clientId);
    }

    @Override
    public boolean clearSession(String clientId) {
        sessionTable.remove(clientId);
        return true;
    }
}
