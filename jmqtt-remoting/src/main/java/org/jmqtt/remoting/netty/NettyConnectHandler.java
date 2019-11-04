package org.jmqtt.remoting.netty;

import org.jmqtt.common.log.LoggerName;
import org.jmqtt.remoting.util.RemotingHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;

/***
 * 客户端"网络"连接处理器
 *  
 * @version
 */
public class NettyConnectHandler extends ChannelDuplexHandler {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.REMOTING);

    private NettyEventExecutor eventExecutor;

    public NettyConnectHandler(NettyEventExecutor nettyEventExcutor){
        this.eventExecutor = nettyEventExcutor;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx){
        final String remoteAddr = RemotingHelper.getRemoteAddr(ctx.channel());
        log.debug("[ChannelActive] -> addr = {}", remoteAddr);
        this.eventExecutor.putNettyEvent(new NettyEvent(remoteAddr, NettyEventType.CONNECT, ctx.channel()));
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx){
        final String remoteAddr = RemotingHelper.getRemoteAddr(ctx.channel());
        log.debug("[ChannelInactive] -> addr = {}", remoteAddr);
        this.eventExecutor.putNettyEvent(new NettyEvent(remoteAddr, NettyEventType.CLOSE, ctx.channel()));
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
        if (evt instanceof IdleStateEvent) {
            IdleStateEvent event = (IdleStateEvent) evt;
            if (event.state().equals(IdleState.ALL_IDLE)) {
                final String remoteAddr = RemotingHelper.getRemoteAddr(ctx.channel());
                log.warn("[HEART_BEAT] -> IDLE exception, addr = {}", remoteAddr);
                RemotingHelper.closeChannel(ctx.channel());
                this.eventExecutor.putNettyEvent(new NettyEvent(remoteAddr, NettyEventType.IDLE, ctx.channel()));
            }
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause){
        String remoteAddr = RemotingHelper.getRemoteAddr(ctx.channel());
        log.warn("Channel caught Exception remotingAddr = {}", remoteAddr);
        log.warn("Channel caught Exception,cause = {}", cause);
        RemotingHelper.closeChannel(ctx.channel());
        this.eventExecutor.putNettyEvent(new NettyEvent(remoteAddr, NettyEventType.EXCEPTION, ctx.channel()));
    }
}
