package org.jmqtt.store.memory;

import org.jmqtt.common.bean.Message;
import org.jmqtt.common.log.LoggerName;
import org.jmqtt.store.OfflineMessageStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

public class DefaultOfflineMessageStore implements OfflineMessageStore {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE);
    /** 离线消息存储 */
    private Map<String/*clientId*/, BlockingQueue<Message>> offlineTable = new ConcurrentHashMap<>();
    /** 每个客户端缓存的最大消息数量 */
    private int msgMaxNum = 1000;

    public DefaultOfflineMessageStore(){
    }

    @Override
    public void clearOfflineMsgCache(String clientId) {
        this.offlineTable.remove(clientId);
    }

    @Override
    public boolean containOfflineMsg(String clientId) {
        return offlineTable.containsKey(clientId);
    }

    @Override
    public boolean addOfflineMessage(String clientId, Message message) {
        if (!this.offlineTable.containsKey(clientId)) {
            synchronized (offlineTable){
                if (!offlineTable.containsKey(clientId)) {
                    BlockingQueue<Message> queue = new ArrayBlockingQueue<>(1000);
                    offlineTable.put(clientId, queue);
                }
            }
        }
        BlockingQueue<Message> queue = this.offlineTable.get(clientId);
        // 离线消息太多则丢弃掉超出部分，先进先扔（FIFO）
        while (queue.size() > msgMaxNum) {
            try {
                queue.take();
            } catch (InterruptedException e) {
                log.warn("[StoreOfflineMessage] -> Store Offline message error,clientId={},msgId={}", clientId, message.getMsgId());
            }
        }
        queue.offer(message);
        
        return true;
    }

    @Override
    public Collection<Message> getAllOfflineMessage(String clientId) {
        BlockingQueue<Message> queue = offlineTable.get(clientId);
        Collection<Message> allMessages = new ArrayList<>();
        int rs = queue.drainTo(allMessages); // 该方法比反复poll更高效
        return allMessages;
    }
}
