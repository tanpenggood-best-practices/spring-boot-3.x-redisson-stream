package com.itplh.best.practices.redisson.controller;

import com.itplh.best.practices.redisson.stream.RedisStreamMetaInfoEnum;
import org.redisson.api.RedissonClient;
import org.redisson.api.stream.StreamAddArgs;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;

@RestController
@RequestMapping("/redisson/stream")
public class RedissonStreamController {

    @Autowired
    private RedissonClient redissonClient;

    @GetMapping("/push-data")
    public String pushData() {
        String ymd = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SSS").format(LocalDateTime.now());
        return "Success: " + Arrays.asList(
                redissonClient.getStream(RedisStreamMetaInfoEnum.STREAM_MESSAGE_STRING.getStream())
                        .add(StreamAddArgs.entry(RedisStreamMetaInfoEnum.STREAM_MESSAGE_STRING.getEventKey(), ymd)),
                redissonClient.getStream(RedisStreamMetaInfoEnum.STREAM_MESSAGE_MAP.getStream())
                        .add(StreamAddArgs.entry(RedisStreamMetaInfoEnum.STREAM_MESSAGE_MAP.getEventKey(), Collections.singletonMap("k", ymd))),
                redissonClient.getStream(RedisStreamMetaInfoEnum.STREAM_MESSAGE_DATE.getStream())
                        .add(StreamAddArgs.entry(RedisStreamMetaInfoEnum.STREAM_MESSAGE_DATE.getEventKey(), new Date()))
        );
    }

}
