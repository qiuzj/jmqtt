package org.jmqtt.remoting.session;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 客户端连接管理器.
 *  
 * @version
 */
public class ConnectManager {
	/** 客户端会话缓存. 格式：<客户端标识, ClientSession> */
    private Map<String, ClientSession> clientCache = new ConcurrentHashMap<>();

    private static final ConnectManager INSTANCE = new ConnectManager();

    private ConnectManager() {};

    public static  ConnectManager getInstance() {
        return INSTANCE;
    }

    public ClientSession getClient(String clientId) {
        return this.clientCache.get(clientId);
    }

    public ClientSession putClient(String clientId, ClientSession clientSession) {
        return this.clientCache.put(clientId, clientSession);
    }

    public boolean containClient(String clientId) {
        return this.clientCache.containsKey(clientId);
    }

    public ClientSession removeClient(String clientId) {
        return this.clientCache.remove(clientId);
    }
}
