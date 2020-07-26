package org.jmqtt.remoting.netty;

import org.jmqtt.common.log.LoggerName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Netty事件执行器. 处理4种事件：连接、断连、空闲、异常. 将事件分发到监听器中进行相应处理.<br>
 * 这个类，一方面用于存储事件，另一方面是为了异步执行事件.
 *  
 * @version
 */
public class NettyEventExecutor implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.REMOTING);
    /** 事件队列 */
    private LinkedBlockingQueue<NettyEvent> eventQueue = new LinkedBlockingQueue<>();
    private final int maxSize = 1000;
    /** 事件监听器. ClientLifeCycleHookService */
    private ChannelEventListener listener;
    boolean stoped = false;
    private Thread thread;

    public NettyEventExecutor(ChannelEventListener channelEventListener){
        this.listener = channelEventListener;
    }

    /**
     * 将新的事件存入事件队列待处理
     *  
     * @param nettyEvent
     */
    public void putNettyEvent(final NettyEvent nettyEvent){
        if (this.eventQueue.size() <= maxSize) {
            this.eventQueue.add(nettyEvent);
        } else {
            log.warn("[NettyEvent] -> event queue size[{}] enough, so drop this event {}", this.eventQueue.size(), nettyEvent.toString());
        }
    }

    @Override
    public void run() {
        while (!this.stoped) {
            try {
                NettyEvent nettyEvent = this.eventQueue.poll(3000, TimeUnit.MILLISECONDS);
                if (nettyEvent != null && listener != null) {
                	// 处理4种事件
                    switch (nettyEvent.getEventType()) {
                        case CONNECT: // 连接事件
                            listener.onChannelConnect(nettyEvent.getRemoteAddr(), nettyEvent.getChannel());
                            break;
                        case CLOSE:  // 断开连接事件
                            listener.onChannelClose(nettyEvent.getRemoteAddr(), nettyEvent.getChannel());
                            break;
                        case EXCEPTION: // 异常事件
                            listener.onChannelException(nettyEvent.getRemoteAddr(), nettyEvent.getChannel());
                            break;
                        case IDLE: // 空闲事件
                            listener.onChannelIdle(nettyEvent.getRemoteAddr(), nettyEvent.getChannel());
                            break;
                         default:
                             break;
                    }
                }
            } catch(Throwable t) {
                log.warn("[NettyEvent] -> service has exception. ", t);
            }
        }
        log.info("[NettyEvent] -> NettyEventExcutor service end");
    }

    /**
     * 启动处理事件的线程
     */
    public void start() {
        this.thread = new Thread(this);
        this.thread.start();
    }

    /**
     * 停止线程
     */
    public void shutdown(){
        this.stoped = true;
    }
}
