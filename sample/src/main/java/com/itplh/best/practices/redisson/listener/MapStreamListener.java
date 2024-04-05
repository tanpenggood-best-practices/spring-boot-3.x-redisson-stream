package com.itplh.best.practices.redisson.listener;

import com.itplh.best.practices.redisson.stream.AbstractAutoRetryStreamListener;
import com.itplh.best.practices.redisson.stream.RedisStreamMetaInfoEnum;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.stereotype.Component;

import java.util.Map;

@Slf4j
@Component
public class MapStreamListener extends AbstractAutoRetryStreamListener<String, MapRecord<String, String, Map>> {

    @Override
    public void doOnMessage(MapRecord<String, String, Map> message) {
        Map<String, Map> msgMap = message.getValue();

        // do something
        double random = Math.random();
        if (random > 0.5) {
            log.warn("consume failure, consumer={} message={} random={}", getFullName(), message, random);
            throw new RuntimeException("Message processing failed");
        }
        log.info("consume success, consumer={} message={}", getFullName(), message);
    }

    @Override
    public RedisStreamMetaInfoEnum getMetaInfo() {
        return RedisStreamMetaInfoEnum.STREAM_MESSAGE_MAP;
    }

}
