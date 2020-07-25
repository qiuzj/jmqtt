package org.jmqtt.broker.acl.impl;

import org.jmqtt.broker.acl.ConnectPermission;

/**
 * 客户端连接权限校验
 */
public class DefaultConnectPermission implements ConnectPermission {

    /**
     * 验证客户端标识
     *
     * @param clientId
     * @return
     */
    @Override
    public boolean clientIdVerfy(String clientId) {
        return true;
    }

    /**
     * 是否为黑名单
     *
     * @param remoteAddr
     * @param clientId
     * @return
     */
    @Override
    public boolean onBlacklist(String remoteAddr, String clientId) {
        return false;
    }

    /**
     * 认证账号密码
     *
     * @param clientId
     * @param userName
     * @param password
     * @return
     */
    @Override
    public boolean authentication(String clientId, String userName, byte[] password) {
        return true;
    }

    /**
     * 验证心跳时间
     *
     * @param clientId
     * @param time
     * @return
     */
    @Override
    public boolean verfyHeartbeatTime(String clientId, int time) {
        return true;
    }
}
