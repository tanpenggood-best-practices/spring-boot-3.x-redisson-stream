package com.itplh.best.practices.redisson.stream;

import org.springframework.data.redis.connection.stream.Record;
import org.springframework.data.redis.stream.StreamListener;

public interface EnhanceStreamListener<K, V extends Record<K, ?>> extends StreamListener<K, V> {

    RedisStreamMetaInfoEnum getMetaInfo();

    default String getStream() {
        return getMetaInfo().getStream();
    }

    default String getGroup() {
        return getMetaInfo().getGroup();
    }

    default String getConsumerName() {
        return getMetaInfo().getConsumer();
    }

    default String getFullName() {
        return getStream() + ":" + getGroup() + ":" + getConsumerName();
    }

    default String getFullGroup() {
        return getStream() + ":" + getGroup();
    }

}
