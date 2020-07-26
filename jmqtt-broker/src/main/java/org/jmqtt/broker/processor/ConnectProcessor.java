package org.jmqtt.broker.processor;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.mqtt.*;
import io.netty.handler.timeout.IdleStateHandler;
import org.jmqtt.broker.BrokerController;
import org.jmqtt.broker.acl.ConnectPermission;
import org.jmqtt.broker.recover.ReSendMessageService;
import org.jmqtt.broker.subscribe.SubscriptionMatcher;
import org.jmqtt.remoting.session.ClientSession;
import org.jmqtt.common.bean.Message;
import org.jmqtt.common.bean.MessageHeader;
import org.jmqtt.common.bean.Subscription;
import org.jmqtt.common.log.LoggerName;
import org.jmqtt.remoting.netty.RequestProcessor;
import org.jmqtt.remoting.session.ConnectManager;
import org.jmqtt.remoting.util.MessageUtil;
import org.jmqtt.remoting.util.NettyUtil;
import org.jmqtt.remoting.util.RemotingHelper;
import org.jmqtt.store.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * 连接报文处理器
 */
public class ConnectProcessor implements RequestProcessor {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.CLIENT_TRACE);

    private FlowMessageStore flowMessageStore;
    private WillMessageStore willMessageStore;
    private OfflineMessageStore offlineMessageStore;
    private SubscriptionStore subscriptionStore;
    private SessionStore sessionStore;
    private ConnectPermission connectPermission;
    private ReSendMessageService reSendMessageService;
    private SubscriptionMatcher subscriptionMatcher;

    public ConnectProcessor(BrokerController brokerController){
        this.flowMessageStore = brokerController.getFlowMessageStore();
        this.willMessageStore = brokerController.getWillMessageStore();
        this.offlineMessageStore = brokerController.getOfflineMessageStore();
        this.subscriptionStore = brokerController.getSubscriptionStore();
        this.sessionStore = brokerController.getSessionStore();
        this.connectPermission = brokerController.getConnectPermission();
        this.reSendMessageService = brokerController.getReSendMessageService();
        this.subscriptionMatcher = brokerController.getSubscriptionMatcher();
    }

    @Override
    public void processRequest(ChannelHandlerContext ctx, MqttMessage mqttMessage) {
        MqttConnectMessage connectMessage = (MqttConnectMessage) mqttMessage;
        MqttConnectReturnCode returnCode = null;

        int mqttVersion = connectMessage.variableHeader().version();
        String clientId = connectMessage.payload().clientIdentifier();
        boolean cleanSession = connectMessage.variableHeader().isCleanSession();
        String userName = connectMessage.payload().userName();
        byte[] password = connectMessage.payload().passwordInBytes();
        ClientSession clientSession = null;
        boolean sessionPresent = false;
        
        try {
            // 1.客户端合法性检查
            if (!versionValid(mqttVersion)) { // 验证协议版本号
                returnCode = MqttConnectReturnCode.CONNECTION_REFUSED_UNACCEPTABLE_PROTOCOL_VERSION;
            } else if (!clientIdVerfy(clientId)) { // 验证客户端标识
                returnCode = MqttConnectReturnCode.CONNECTION_REFUSED_IDENTIFIER_REJECTED;
            } else if (onBlackList(RemotingHelper.getRemoteAddr(ctx.channel()), clientId)) { // 黑名单检查
                returnCode = MqttConnectReturnCode.CONNECTION_REFUSED_NOT_AUTHORIZED;
            } else if (!authentication(clientId, userName, password)) { // 账号和密码验证
                returnCode = MqttConnectReturnCode.CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD;
            } else {
            // 2.客户端连接、会话缓存

                int heartbeatSec = connectMessage.variableHeader().keepAliveTimeSeconds();
                if (!keepAlive(clientId, ctx, heartbeatSec)) {
                    log.warn("[CONNECT] -> set heartbeat failure,clientId:{},heartbeatSec:{}", clientId, heartbeatSec);
                    throw new Exception("set heartbeat failure");
                }
                
                Object lastState = sessionStore.getLastSession(clientId);
                // 如果客户端在线，则关闭连接、清理会话
                if (Objects.nonNull(lastState) && lastState.equals(true)) {
                    // TODO cluster clear and disconnect previous connect
                    ClientSession previousClient = ConnectManager.getInstance().getClient(clientId);
                    if (previousClient != null) {
                    	// 关闭客户端clientId之前的连接
                        previousClient.getCtx().close();
                        // 清理会话缓存
                        ConnectManager.getInstance().removeClient(clientId);
                    }
                }
                
                // 如果清理会话，则新建会话，同时要清理存储
                if (cleanSession) {
                    clientSession = createNewClientSession(clientId, ctx);
                    sessionPresent = false;
                // 如果不清理会话
                } else {
                	// 有存储会话状态，则重新加载
                    if (Objects.nonNull(lastState)) {
                        clientSession = reloadClientSession(ctx, clientId);
                        sessionPresent = true;
                    } else { // 没有会话状态，则新建会话，不需要清理存储
                        clientSession = new ClientSession(clientId, false, ctx);
                        sessionPresent = false;
                    }
                }
                // 客户端在线状态
                sessionStore.setSession(clientId, true);
                
                boolean willFlag = connectMessage.variableHeader().isWillFlag();
                // 存储遗嘱消息
                if (willFlag) {
                    boolean willRetain = connectMessage.variableHeader().isWillRetain();
                    int willQos = connectMessage.variableHeader().willQos();
                    String willTopic = connectMessage.payload().willTopic();
                    byte[] willPayload = connectMessage.payload().willMessageInBytes();
                    storeWillMsg(clientId, willRetain, willQos, willTopic, willPayload);
                }
                returnCode = MqttConnectReturnCode.CONNECTION_ACCEPTED;
                // 将连接Channel与客户端标识进行关联，保存到本地缓存
                NettyUtil.setClientId(ctx.channel(), clientId);
                // 客户端会话缓存，保存到本地缓存
                ConnectManager.getInstance().putClient(clientId, clientSession);
            }

            // 3.客户端连接响应
            // CONNACK – 确认连接请求
            MqttConnAckMessage ackMessage = MessageUtil.getConnectAckMessage(returnCode, sessionPresent);
            ctx.writeAndFlush(ackMessage);
            log.info("[CONNECT] -> {} connect to this mqtt server", clientId);

            // 4.离线消息重发. 客户端ID存入待重发消息队列
            reConnect2SendMessage(clientId);
        } catch (Exception ex) {
            log.warn("[CONNECT] -> Service Unavailable: cause={}", ex);
            returnCode = MqttConnectReturnCode.CONNECTION_REFUSED_SERVER_UNAVAILABLE;
            MqttConnAckMessage ackMessage = MessageUtil.getConnectAckMessage(returnCode, sessionPresent);
            ctx.writeAndFlush(ackMessage);
        }
    }

    /**
     * 更新keepAlive时间
     *  
     * @param clientId
     * @param ctx
     * @param heatbeatSec
     * @return
     */
    private boolean keepAlive(String clientId, ChannelHandlerContext ctx, int heatbeatSec) {
        if (this.connectPermission.verfyHeartbeatTime(clientId, heatbeatSec)) {
            int keepAlive = (int) (heatbeatSec * 1.5f);
            if (ctx.pipeline().names().contains("idleStateHandler")) {
                ctx.pipeline().remove("idleStateHandler");
            }
            // 使用新的超时时间
            ctx.pipeline().addFirst("idleStateHandler", new IdleStateHandler(keepAlive, 0, 0));
            return true;
        }
        return false;
    }

    /**
     * 存储遗嘱消息
     *  
     * @param clientId
     * @param willRetain
     * @param willQos
     * @param willTopic
     * @param willPayload
     */
    private void storeWillMsg(String clientId, boolean willRetain, int willQos, String willTopic, byte[] willPayload){
        Map<String,Object> headers = new HashMap<>();
        headers.put(MessageHeader.RETAIN, willRetain);
        headers.put(MessageHeader.QOS, willQos);
        headers.put(MessageHeader.TOPIC, willTopic);
        headers.put(MessageHeader.WILL, true);
        Message message = new Message(Message.Type.WILL, headers, willPayload);
        message.setClientId(clientId);
        willMessageStore.storeWillMessage(clientId, message);
        log.info("[WillMessageStore] : {} store will message:{}", clientId, message);
    }

    /**
     * 创建新的会话. 清理掉存储中的离线消息、订阅信息、发送中的消息等
     *  
     * @param clientId
     * @param ctx
     * @return
     */
    private ClientSession createNewClientSession(String clientId, ChannelHandlerContext ctx){
        ClientSession clientSession = new ClientSession(clientId, true);
        clientSession.setCtx(ctx);
        //clear previous sessions
        this.flowMessageStore.clearClientFlowCache(clientId);
        this.offlineMessageStore.clearOfflineMsgCache(clientId);
        this.subscriptionStore.clearSubscription(clientId);
        this.sessionStore.clearSession(clientId);
        return clientSession;
    }

    /**
     * cleansession is false, reload client session
     */
    private ClientSession reloadClientSession(ChannelHandlerContext ctx,String clientId) {
        ClientSession clientSession = new ClientSession(clientId, false);
        clientSession.setCtx(ctx);
        Collection<Subscription> subscriptions = subscriptionStore.getSubscriptions(clientId);
        for (Subscription subscription : subscriptions) {
            this.subscriptionMatcher.subscribe(subscription);
        }
        return clientSession;
    }

    /**
     * 离线消息重发.<br>
     * 每次有客户端连接上线后，将客户端ID存入重发消息队列，待进一步确认是否有离线消息，如有则重发消息.
     *
     * @param clientId
     */
    private void reConnect2SendMessage(String clientId) {
        this.reSendMessageService.put(clientId);
        this.reSendMessageService.wakeUp();
    }

    /**
     * 账号和密码验证
     *  
     * @param clientId
     * @param username
     * @param password
     * @return
     */
    private boolean authentication(String clientId, String username, byte[] password) {
        return this.connectPermission.authentication(clientId, username, password);
    }

    /**
     * 黑名单检查
     *  
     * @param remoteAddr
     * @param clientId
     * @return
     */
    private boolean onBlackList(String remoteAddr, String clientId) {
        return this.connectPermission.onBlacklist(remoteAddr, clientId);
    }

    /**
     * 验证客户端标识
     *  
     * @param clientId
     * @return
     */
    private boolean clientIdVerfy(String clientId) {
        return this.connectPermission.clientIdVerfy(clientId);
    }

    /**
     * 验证协议版本
     *  
     * @param mqttVersion
     * @return
     */
    private boolean versionValid(int mqttVersion) {
        if (mqttVersion == 3 || mqttVersion == 4) {
            return true;
        }
        return false;
    }

}
