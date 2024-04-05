package com.itplh.best.practices.redisson.stream;

import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RStream;
import org.redisson.api.RedissonClient;
import org.redisson.api.StreamMessageId;
import org.redisson.api.stream.StreamAddArgs;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.RecordId;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

@Slf4j
public abstract class AbstractAutoRetryStreamListener<S, V extends MapRecord<S, String, ?>> implements EnhanceStreamListener<S, V> {

    @Autowired
    private RedissonClient redissonClient;

    /**
     * key = fullGroup + RecordId
     * value = retries
     */
    private static Map<String, LongAdder> retryMap = new ConcurrentHashMap<>();

    public abstract void doOnMessage(V v);

    @Override
    public void onMessage(V message) {
        try {
            doOnMessage(message);
            try {
                onSuccess(message);
            } catch (Throwable e) {
                log.error(e.getMessage(), e);
            }
        } catch (Throwable e) {
            // XADD cannot send messages to the specified group,
            // don't enable retry mechanism when the stream has multiple consume groups
            RStream<String, Object> rStream = redissonClient.getStream(getStream());
            if (rStream.listGroups().size() > 1) {
                onFailure(message, e);
                return;
            }
            // Consume failure, reaching maximum retry count
            LongAdder retries = retryMap.getOrDefault(retryKey(message.getId()), new LongAdder());
            if (retries.longValue() >= maxRetries()) {
                onFailure(message, e);
                return;
            }
            // Add the message to the Stream queue, once again
            StreamMessageId newMessageId = rStream.add(StreamAddArgs.entries((Map) message.getValue()));
            // update retries
            retries.increment();
            retryMap.put(retryKey(newMessageId), retries);
        } finally {
            RStream<String, V> rStream = redissonClient.getStream(getStream());
            StreamMessageId messageId = convert(message.getId());
            // clear retry
            retryMap.remove(retryKey(messageId));
            // Submit ACK
            rStream.ack(getGroup(), messageId);
            // delete message, release memory
            rStream.remove(messageId);
            // hooks
            onFinally(message);
        }
    }

    /**
     * max retries
     * default：10
     * The implementation class can customize the maximum number of retries by overriding this method
     */
    protected long maxRetries() {
        return 10L;
    }

    /**
     * hooks, triggered when consume success
     *
     * @param message
     */
    protected void onSuccess(V message) {
    }

    /**
     * hooks, triggered when consume failure
     *
     * @param message
     * @param e
     */
    protected void onFailure(V message, Throwable e) {
        saveToDeadLetterQueue(message);

        LongAdder retriesCount = retryMap.getOrDefault(retryKey(message.getId()), new LongAdder());
        log.error("message is put to dead letter queue, consumer={} message={} retries={} error={}", getFullName(), message.getValue(), retriesCount, e.getMessage(), e);
    }

    /**
     * hooks, triggered when finally end
     *
     * @param message
     */
    protected void onFinally(V message) {
    }

    private String retryKey(RecordId recordId) {
        return retryKey(convert(recordId));
    }

    private String retryKey(StreamMessageId messageId) {
        return getFullGroup() + "-" + messageId.toString();
    }

    private StreamMessageId convert(RecordId recordId) {
        return new StreamMessageId(recordId.getTimestamp(), recordId.getSequence());
    }

    /**
     * save message to dead letter queue
     *
     * @param message
     */
    private void saveToDeadLetterQueue(V message) {
        String key = "my:hash:dead-letter-queue";
        String hk = getFullName() + ":" + message.getId();
        Object hv = message.getValue();
        redissonClient.getMap(key).put(hk, hv);
    }

}
